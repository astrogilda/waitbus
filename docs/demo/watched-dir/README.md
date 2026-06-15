# watched-dir

waitbus's `fs_watch` source observes file changes in this directory.

Touch any file:

```bash
touch watched-dir/$(date +%s)
```

A downstream subscriber blocking on `waitbus wait --source fs` unblocks
the moment the touch lands.

The `.gitkeep` next to this README is what keeps the directory in
revision control between demo runs; it has no other purpose.
