/*
 * faultshim.c — test-only libc interposer for the bridge worker's failure paths
 *
 * `LD_PRELOAD` onto a test cluster's own `pg_ctl`, never onto anything
 * process-global. Wrappers act only on calls made from `walshadow.so`, or on
 * descriptors that module created, so postmaster and backend sockets pass
 * through untouched. Not linked into `walshadow.so` and not installed.
 *
 * Two files, both named by environment:
 *
 *   WS_FAULT_SCRIPT  one rule per line, `<op> <nth> <times> <action> <arg>`
 *                    nth is a 1-based occurrence of that op, times 0 is
 *                    unlimited, arg is an errno for `fail` and a byte count
 *                    for `short`
 *   WS_FAULT_STATE   fixed-size counters, shared through `mmap`, so occurrence
 *                    numbering survives a worker restart and the fixture can
 *                    see what was consumed
 *
 * The fixture bumps the generation word to rearm: rules reload and occurrence
 * counts restart, which is what lets one cluster serve many scenarios.
 * Layout is duplicated in tests/common/faults.rs; keep the op order in step.
 */
#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define WS_FAULT_MAGIC		0x57534831	/* "WSH1" */
#define WS_MAX_RULES		16
#define WS_MAX_FDS			4096

/* Order is wire state, shared with the Rust fixture */
enum ws_op
{
	WS_OP_SOCKET = 0,
	WS_OP_BIND,
	WS_OP_LISTEN,
	WS_OP_CONNECT,
	WS_OP_ACCEPT,
	WS_OP_RECV,
	WS_OP_SEND,
	WS_OP_CHMOD,
	WS_OP_FCNTL,
	WS_OP_CLOSE,
	WS_OP_SETSOCKOPT,
	WS_N_OPS
};

static const char *const ws_op_names[WS_N_OPS] = {
	"socket", "bind", "listen", "connect", "accept",
	"recv", "send", "chmod", "fcntl", "close", "setsockopt"
};

enum ws_action
{
	WS_ACT_FAIL = 0,			/* -1 with the scripted errno */
	WS_ACT_ZERO,				/* 0 bytes on a positive length */
	WS_ACT_SHORT				/* clamp the real call's length */
};

static const char *const ws_action_names[] = {"fail", "zero", "short"};

struct ws_rule
{
	int			op;
	uint32_t	nth;
	uint32_t	times;
	int			action;
	int			arg;
};

struct ws_state
{
	uint32_t	magic;
	uint32_t	generation;
	uint32_t	rules_parsed;
	uint32_t	reserved;
	uint32_t	op_seen[WS_N_OPS];
	uint32_t	consumed[WS_MAX_RULES];
};

/* descriptor roles, the only ones a rule can name */
#define WS_FD_LISTEN_OR_PROBE	1
#define WS_FD_CONN				2

static int	ws_ready;
static struct ws_state *ws_state;
static struct ws_rule ws_rules[WS_MAX_RULES];
static int	ws_nrules;
static uint32_t ws_generation;
static unsigned char ws_fd_tag[WS_MAX_FDS];

static int	(*ws_real_socket) (int, int, int);
static int	(*ws_real_bind) (int, const struct sockaddr *, socklen_t);
static int	(*ws_real_listen) (int, int);
static int	(*ws_real_connect) (int, const struct sockaddr *, socklen_t);
static int	(*ws_real_accept) (int, struct sockaddr *, socklen_t *);
static ssize_t (*ws_real_recv) (int, void *, size_t, int);
static ssize_t (*ws_real_send) (int, const void *, size_t, int);
static int	(*ws_real_chmod) (const char *, mode_t);
static int	(*ws_real_fcntl) (int, int, ...);
static int	(*ws_real_close) (int);
static int	(*ws_real_setsockopt) (int, int, int, const void *, socklen_t);

static void
ws_die(const char *what)
{
	ssize_t		ignored = write(2, what, strlen(what));

	(void) ignored;
	abort();
}

static void *
ws_next(const char *name)
{
	void	   *p = dlsym(RTLD_NEXT, name);

	if (p == NULL)
		ws_die("walshadow faultshim: unresolved libc symbol\n");
	return p;
}

static void
ws_init(void)
{
	const char *path;
	void	   *map;
	int			fd;

	ws_ready = 1;				/* first: dlsym reaches back through here */
	ws_real_socket = ws_next("socket");
	ws_real_bind = ws_next("bind");
	ws_real_listen = ws_next("listen");
	ws_real_connect = ws_next("connect");
	ws_real_accept = ws_next("accept");
	ws_real_recv = ws_next("recv");
	ws_real_send = ws_next("send");
	ws_real_chmod = ws_next("chmod");
	ws_real_fcntl = ws_next("fcntl");
	ws_real_close = ws_next("close");
	ws_real_setsockopt = ws_next("setsockopt");

	path = getenv("WS_FAULT_STATE");
	if (path == NULL)
		return;
	fd = open(path, O_RDWR);
	if (fd < 0)
		return;
	map = mmap(NULL, sizeof(struct ws_state), PROT_READ | PROT_WRITE,
			   MAP_SHARED, fd, 0);
	ws_real_close(fd);
	if (map == MAP_FAILED)
		return;
	ws_state = (struct ws_state *) map;
}

static int
ws_name_index(const char *const *names, int n, const char *want)
{
	int			i;

	for (i = 0; i < n; i++)
		if (strcmp(names[i], want) == 0)
			return i;
	return -1;
}

static void
ws_reload(void)
{
	const char *path = getenv("WS_FAULT_SCRIPT");
	char		line[256];
	FILE	   *f;

	ws_nrules = 0;
	if (path == NULL)
		return;
	f = fopen(path, "r");
	if (f == NULL)
		return;
	while (ws_nrules < WS_MAX_RULES && fgets(line, sizeof(line), f) != NULL)
	{
		char		op[32];
		char		action[32];
		unsigned	nth;
		unsigned	times;
		int			arg;
		struct ws_rule *r;

		if (sscanf(line, "%31s %u %u %31s %d",
				   op, &nth, &times, action, &arg) != 5)
			continue;
		r = &ws_rules[ws_nrules];
		r->op = ws_name_index(ws_op_names, WS_N_OPS, op);
		r->action = ws_name_index(ws_action_names, 3, action);
		if (r->op < 0 || r->action < 0)
			ws_die("walshadow faultshim: bad rule\n");
		r->nth = nth;
		r->times = times;
		r->arg = arg;
		ws_nrules++;
	}
	fclose(f);
}

/*
 * Count this call against its op and hand back the rule that claims it, at
 * most one per call. Counters live in the shared file, so a rule armed for the
 * second `bind` is still the second `bind` after the worker restarts.
 */
static const struct ws_rule *
ws_match(int op)
{
	uint32_t	seen;
	int			i;

	if (ws_state->magic != WS_FAULT_MAGIC)
		return NULL;
	if (ws_state->generation != ws_generation)
	{
		ws_generation = ws_state->generation;
		ws_reload();
		ws_state->rules_parsed = (uint32_t) ws_nrules;
	}
	seen = ++ws_state->op_seen[op];
	for (i = 0; i < ws_nrules; i++)
	{
		const struct ws_rule *r = &ws_rules[i];

		if (r->op != op || seen < r->nth)
			continue;
		if (r->times != 0 && ws_state->consumed[i] >= r->times)
			continue;
		ws_state->consumed[i]++;
		return r;
	}
	return NULL;
}

static int
ws_from_module(void *ra)
{
	Dl_info		info;
	const char *base;

	if (dladdr(ra, &info) == 0 || info.dli_fname == NULL)
		return 0;
	base = strrchr(info.dli_fname, '/');
	base = base != NULL ? base + 1 : info.dli_fname;
	return strcmp(base, "walshadow.so") == 0;
}

/*
 * `true` when the call is the module's and scripting is live, so the wrapper
 * owns the descriptor bookkeeping too. Bookkeeping never disturbs errno.
 */
static int
ws_hook(void *ra, int op, const struct ws_rule **rule)
{
	int			save_errno = errno;

	*rule = NULL;
	if (!ws_ready)
		ws_init();
	if (ws_state == NULL || !ws_from_module(ra))
	{
		errno = save_errno;
		return 0;
	}
	*rule = ws_match(op);
	errno = save_errno;
	return 1;
}

/* Scripting keyed on a descriptor role rather than the caller: PostgreSQL's
 * own pg_set_noblock and closesocket live in the server binary, which
 * LD_PRELOAD cannot reach into */
static const struct ws_rule *
ws_hook_fd(int fd, int op)
{
	const struct ws_rule *rule = NULL;
	int			save_errno = errno;

	if (!ws_ready)
		ws_init();
	if (ws_state != NULL && fd >= 0 && fd < WS_MAX_FDS && ws_fd_tag[fd] != 0)
		rule = ws_match(op);
	errno = save_errno;
	return rule;
}

static void
ws_tag(int fd, unsigned char role)
{
	if (fd >= 0 && fd < WS_MAX_FDS)
		ws_fd_tag[fd] = role;
}

int
socket(int domain, int type, int protocol)
{
	const struct ws_rule *rule;
	int			fd;

	if (!ws_hook(__builtin_return_address(0), WS_OP_SOCKET, &rule))
		return ws_real_socket(domain, type, protocol);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	fd = ws_real_socket(domain, type, protocol);
	ws_tag(fd, WS_FD_LISTEN_OR_PROBE);
	return fd;
}

int
bind(int fd, const struct sockaddr *addr, socklen_t len)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_BIND, &rule))
		return ws_real_bind(fd, addr, len);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return ws_real_bind(fd, addr, len);
}

int
listen(int fd, int backlog)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_LISTEN, &rule))
		return ws_real_listen(fd, backlog);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return ws_real_listen(fd, backlog);
}

int
connect(int fd, const struct sockaddr *addr, socklen_t len)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_CONNECT, &rule))
		return ws_real_connect(fd, addr, len);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return ws_real_connect(fd, addr, len);
}

int
accept(int fd, struct sockaddr *addr, socklen_t *len)
{
	const struct ws_rule *rule;
	int			conn;

	if (!ws_hook(__builtin_return_address(0), WS_OP_ACCEPT, &rule))
		return ws_real_accept(fd, addr, len);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	conn = ws_real_accept(fd, addr, len);
	ws_tag(conn, WS_FD_CONN);
	return conn;
}

ssize_t
recv(int fd, void *buf, size_t len, int flags)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_RECV, &rule))
		return ws_real_recv(fd, buf, len, flags);
	if (rule != NULL)
	{
		if (rule->action == WS_ACT_FAIL)
		{
			errno = rule->arg;
			return -1;
		}
		if (rule->action == WS_ACT_ZERO)
			return 0;
		if ((size_t) rule->arg < len)
			len = (size_t) rule->arg;
	}
	return ws_real_recv(fd, buf, len, flags);
}

ssize_t
send(int fd, const void *buf, size_t len, int flags)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_SEND, &rule))
		return ws_real_send(fd, buf, len, flags);
	if (rule != NULL)
	{
		if (rule->action == WS_ACT_FAIL)
		{
			errno = rule->arg;
			return -1;
		}
		/* Zero on a positive length is injected, not something Linux does;
		 * errno stays whatever it was, which is the point of the case */
		if (rule->action == WS_ACT_ZERO)
			return 0;
		if ((size_t) rule->arg < len)
			len = (size_t) rule->arg;
	}
	return ws_real_send(fd, buf, len, flags);
}

int
chmod(const char *path, mode_t mode)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_CHMOD, &rule))
		return ws_real_chmod(path, mode);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return ws_real_chmod(path, mode);
}

int
setsockopt(int fd, int level, int name, const void *val, socklen_t len)
{
	const struct ws_rule *rule;

	if (!ws_hook(__builtin_return_address(0), WS_OP_SETSOCKOPT, &rule))
		return ws_real_setsockopt(fd, level, name, val, len);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return ws_real_setsockopt(fd, level, name, val, len);
}

int
fcntl(int fd, int cmd, ...)
{
	const struct ws_rule *rule;
	va_list		ap;
	void	   *arg;

	va_start(ap, cmd);
	arg = va_arg(ap, void *);
	va_end(ap);

	rule = ws_hook_fd(fd, WS_OP_FCNTL);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return ws_real_fcntl(fd, cmd, arg);
}

/*
 * A close that reports a failure still closed the descriptor; what the
 * scenario is about is the errno it leaves behind for the caller's `%m`.
 */
int
close(int fd)
{
	const struct ws_rule *rule;
	int			rc;

	rule = ws_hook_fd(fd, WS_OP_CLOSE);
	if (fd >= 0 && fd < WS_MAX_FDS)
		ws_fd_tag[fd] = 0;
	rc = ws_real_close(fd);
	if (rule != NULL && rule->action == WS_ACT_FAIL)
	{
		errno = rule->arg;
		return -1;
	}
	return rc;
}
