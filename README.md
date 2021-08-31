# About this application
Page-count is a dockerized application, that can stream a specific folder location, namely
`source`(**this folder is present in this page-view-counts**)
and calculate the `7days page view count` and `7day user-page view count`.

These counts/aggregations are stored in a delta path, `page_count_aggregations`, that will get
created in the mount location when docker is run.

**Streaming is currently implemented as a batch process**

# Build docker image
```
cd <path/to/code/page-view-counts>
docker build -t myimage .
```

# Run page-count application
Please Update `<path/to/code/page-view-counts>` to the path that the code is cloned to.

```
docker run -it -v <path/to/code/page-view-counts>/.ivy:/home/jovyan/.ivy2 -v <path/to/code/page-view-counts>:/home/jovyan/page_count page-count
```

# Run unit tests
```
docker run -it -v <path/to/code/page-view-counts>/.ivy:/home/jovyan/.ivy2 page-count python -m pytest tests
```

# TODO
* Handle exceptions better
