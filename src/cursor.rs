//! Little-endian reader over PG WAL payloads

use walrus::pg::walparser::RelFileNode;

/// Reads consume the front, `reserve` trims trailing payloads off the back
pub(crate) trait Cursor {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]>;
    fn skip(&mut self, n: usize) -> Option<()>;
    fn reserve(&mut self, n: usize) -> Option<()>;

    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|[b]| b)
    }

    fn u16(&mut self) -> Option<u16> {
        self.take().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }

    fn i32(&mut self) -> Option<i32> {
        self.take().map(i32::from_le_bytes)
    }

    /// PG `RelFileLocator`
    fn rel_file_node(&mut self) -> Option<RelFileNode> {
        Some(RelFileNode {
            spc_node: self.u32()?,
            db_node: self.u32()?,
            rel_node: self.u32()?,
        })
    }
}

impl Cursor for &[u8] {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.split_first_chunk::<N>()?;
        *self = rest;
        Some(*head)
    }

    fn skip(&mut self, n: usize) -> Option<()> {
        *self = self.get(n..)?;
        Some(())
    }

    fn reserve(&mut self, n: usize) -> Option<()> {
        *self = self.get(..self.len().checked_sub(n)?)?;
        Some(())
    }
}
