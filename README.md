# toykv

An LSM key-value store created so I could learn the basics of writing an LSM.
It's purely a learning exercise and not suited to being used in any real world
application.

It does almost nothing you'd want an actual place you put data to do:

- Doesn't bother checksumming any data. Bit flips for the win!
- ~~Opens files every time it reads or writes them. Every. Single. Time.~~ Fixed
  with
  [c9006a4](https://github.com/mikerhodes/toykv/commit/c9006a4047a7e4db68c10752ff1113d8788784dd)
  and
  [34decd2](https://github.com/mikerhodes/toykv/commit/34decd29aea6757269afa5c16713a39f2c9e842f)!
- Makes no attempt at all to compress data. Keys and values are just streamed to
  disk in all their bloated glory.
- It's an LSM "tree" with just one branch of SSTables.
- And those SSTables are never compacted, and eventually you run out of them.

And it does none of those things so I have a chance to explain it in a blog post
that contains a reasonable number of words. For that purpose I think I've been
successful. With a bit of code cleanup, it should be a decent pedagogical tool.
