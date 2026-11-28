#!/usr/bin/awk -f
# Fail when src/ modules import each other in a cycle
# Reads .rs paths on stdin: find src -name '*.rs' | awk -f ci/module_cycles.awk
# Nodes are library files (src/bin excluded), edges are non-test `crate::`,
# `super::`, `self::` and child-relative paths, `use` trees included. A path
# resolves to longest module prefix backed by a file. Paths from a module into
# its own ancestors are skipped: a child naming `super::Item` refers to code its
# parent declares

{ path[++np] = $0 }
/(^|\/)lib\.rs$/ && (root == "" || length($0) - 6 < length(root)) { root = substr($0, 1, length($0) - 6) }

END {
	for (i = 1; i <= np; i++) {
		m = substr(path[i], length(root) + 1)
		if (m ~ /^bin\// || m == "lib.rs") continue
		sub(/\.rs$/, "", m); sub(/(^|\/)mod$/, "", m); gsub(/\//, "::", m)
		file[m] = path[i]; mods[++nm] = m
	}
	sort(mods, nm)
	scan(root "lib.rs", "")
	for (i = 1; i <= nm; i++) scan(file[mods[i]], mods[i])
	for (i = 1; i <= nm; i++) if (!(mods[i] in idx)) visit(mods[i])
	report()
}

function sort(a, n,   i, j, x) {
	for (i = 2; i <= n; i++) {
		x = a[i]
		for (j = i - 1; j && a[j] > x; j--) a[j + 1] = a[j]
		a[j + 1] = x
	}
}

# Blank comments, string and char literals and `#[cfg(test)]` items, then pass
# each line to refs. mod "" collects lib.rs `pub use` aliases instead
function scan(p, mod,   line, o, st, depth, quote, c) {
	st = 0; skip = 0; inuse = 0
	while ((getline line < p) > 0) {
		o = ""
		while (line != "") {
			if (st == 1) {
				if (!match(line, /\/\*|\*\//)) break
				depth += substr(line, RSTART, 2) == "/*" ? 1 : -1
				line = substr(line, RSTART + 2)
				if (!depth) { st = 0; o = o " " }
			} else if (st == 2) {
				if (!match(line, /\\.?|"/)) break
				if (substr(line, RSTART, 1) == "\"") { st = 0; o = o "\"\"" }
				line = substr(line, RSTART + RLENGTH)
			} else if (st == 3) {
				if (!(c = index(line, quote))) break
				line = substr(line, c + length(quote)); st = 0; o = o "\"\""
			} else {
				if (!match(line, /\/[\/*]|["']/)) { o = o line; break }
				o = o substr(line, 1, RSTART - 1); line = substr(line, RSTART)
				c = substr(line, 1, 1)
				if (c == "/") {
					if (substr(line, 2, 1) == "/") break
					st = 1; depth = 1; line = substr(line, 3)
				} else if (c == "\"") {
					if (match(o, /(^|[^A-Za-z0-9_])b?r#*$/)) {
						match(o, /#*$/); quote = "\"" substr(o, RSTART); st = 3
					} else st = 2
					line = substr(line, 2)
				} else if (match(line, /^'(\\.[^']*|[^\\'])'/)) {
					o = o "' '"; line = substr(line, RLENGTH + 1)
				} else { o = o c; line = substr(line, 2) }
			}
		}
		refs(untest(o), mod)
	}
	close(p)
}

function untest(o,   r, t) {
	r = ""
	while (o != "") {
		if (!skip) {
			if (!(t = index(o, "#[cfg(test)]"))) return r o
			r = r substr(o, 1, t - 1); o = substr(o, t + 12); skip = 1; sdepth = 0
		} else {
			if (!match(o, /[{};]/)) break
			t = substr(o, RSTART, 1); o = substr(o, RSTART + 1)
			if (t == "{") sdepth++
			else if (t == ";") { if (!sdepth) skip = 0 }
			else if (!--sdepth) skip = 0
		}
	}
	return r
}

function refs(o, mod,   rest, t, n, i, u) {
	rest = o
	while (match(rest, /(crate|super|self)(::[A-Za-z0-9_]+)+/)) {
		if (mod != "" && (RSTART == 1 || substr(rest, RSTART - 1, 1) !~ /[A-Za-z0-9_]/))
			edge(mod, substr(rest, RSTART, RLENGTH))
		rest = substr(rest, RSTART + RLENGTH)
	}
	while (o != "") {
		if (!inuse) {
			if (!match(o, /(^|[^A-Za-z0-9_])use[ \t]/)) return
			pubuse = substr(o, 1, RSTART + RLENGTH - 5) ~ /(^|[^A-Za-z0-9_])pub(\([^)]*\))?[ \t]*$/
			o = substr(o, RSTART + RLENGTH); inuse = 1; tree = ""
		}
		if (!(t = index(o, ";"))) { tree = tree " " o; return }
		tree = tree " " substr(o, 1, t - 1); o = substr(o, t + 1); inuse = 0
		n = expand(tree, u)
		for (i = 1; i <= n; i++) {
			if (mod != "") edge(mod, u[i])
			else if (pubuse) { t = u[i]; sub(/.*::/, "", t); alias[t] = u[i] }
		}
	}
}

# `a::{b, c::{d, e as f}, g::*}` to a::b a::c::d a::c::e a::g
function expand(tree, u,   n, i, sp, stk, cur, tok, as) {
	n = 0; sp = 1; stk[1] = ""; cur = ""; as = 0
	while (match(tree, /[{},*]|[A-Za-z0-9_:]+/)) {
		tok = substr(tree, RSTART, RLENGTH); tree = substr(tree, RSTART + RLENGTH)
		if (tok == "{") { sp++; stk[sp] = stk[sp - 1] cur; cur = "" }
		else if (tok == "}" || tok == ",") {
			if (cur != "") u[++n] = stk[sp] cur
			cur = ""
			if (tok == "}") sp--
		} else if (tok == "as") as = 1
		else if (as) as = 0
		else if (tok != "*") cur = tok
	}
	if (cur != "") u[++n] = stk[sp] cur
	for (i = 1; i <= n; i++) { sub(/::$/, "", u[i]); sub(/::self$/, "", u[i]) }
	return n
}

function resolve(parts,   first) {
	first = parts; sub(/::.*/, "", first)
	if (first in alias) parts = alias[first] substr(parts, length(first) + 1)
	while (parts != "" && !(parts in file))
		if (!sub(/::[A-Za-z0-9_]+$/, "", parts)) parts = ""
	return parts
}

function join(a, b) { return a == "" ? b : b == "" ? a : a "::" b }

function edge(mod, ref,   seg, n, i, parts, base, t) {
	n = split(ref, seg, "::")
	if (seg[1] == "crate") { parts = ""; i = 2 }
	else if (seg[1] == "self") { parts = mod; i = 2 }
	else if (seg[1] == "super") {
		parts = mod
		for (i = 1; i <= n && seg[i] == "super"; i++) sub(/(^|::)[A-Za-z0-9_]+$/, "", parts)
	} else { parts = mod; i = 1 }
	base = i
	for (; i <= n; i++) parts = join(parts, seg[i])
	t = resolve(parts)
	if (t == "" || t == mod || index(mod "::", t "::") == 1) return
	if (!((mod, t) in syms)) { adj[mod, ++nadj[mod]] = t; syms[mod, t] = "" }
	if (base <= n && index(" " syms[mod, t] " ", " " seg[n] " ") == 0)
		syms[mod, t] = syms[mod, t] (syms[mod, t] == "" ? "" : " ") seg[n]
}

function visit(v,   k, w, c) {
	idx[v] = low[v] = nidx++
	tstk[++tsp] = v; on[v] = 1
	for (k = 1; k <= nadj[v]; k++) {
		w = adj[v, k]
		if (!(w in idx)) { visit(w); if (low[w] < low[v]) low[v] = low[w] }
		else if (on[w] && idx[w] < low[v]) low[v] = idx[w]
	}
	if (low[v] != idx[v]) return
	c = ++ncomp; compn[c] = 0
	do { w = tstk[tsp--]; on[w] = 0; comp[c, ++compn[c]] = w } while (w != v)
}

function report(   c, i, j, k, l, m, b, line, order, members, n, s, sy, ns) {
	n = 0
	for (c = 1; c <= ncomp; c++) if (compn[c] > 1) order[++n] = c
	for (i = 2; i <= n; i++) {
		c = order[i]
		for (j = i - 1; j && compn[order[j]] < compn[c]; j--) order[j + 1] = order[j]
		order[j + 1] = c
	}
	for (i = 1; i <= n; i++) {
		c = order[i]; m = compn[c]
		split("", members); split("", line)
		for (j = 1; j <= m; j++) { line[j] = comp[c, j]; members[comp[c, j]] }
		sort(line, m)
		s = ""; for (j = 1; j <= m; j++) s = s " " line[j]
		print "cycle of " m " files:" s
		for (j = 1; j <= m; j++)
			for (k = 1; k <= nadj[line[j]]; k++) {
				b = adj[line[j], k]
				if (!(b in members)) continue
				ns = split(syms[line[j], b], sy, " "); sort(sy, ns)
				s = sy[1]; for (l = 2; l <= ns && l <= 4; l++) s = s ", " sy[l]
				print "  " line[j] " -> " b " (" s ")"
			}
	}
	if (n) {
		print "Move shared items down into a module both sides already depend on, see AGENTS.md"
		exit 1
	}
}
