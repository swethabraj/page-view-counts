# About this application
Page-count is a dockerized application, that can stream data from [here](https://github.com/swethabraj/page-view-counts/tree/master/source)
and calculate the `7days page view count` and `7day user-page view count`.

These counts/aggregations are stored in a delta path, `page_count_aggregations`, that will get
created in the mount location when application is run.

### Prerequisites

* Docker installed

### How to use

* Clone this repo to your local machine.
* `cd <path/to/code/page-view-counts>`
* Build docker image `docker build -t page-count .`
    
Once the image is built, add files to the [source](https://github.com/swethabraj/page-view-counts/tree/master/source) and then run the application using the following commands

# Run page-count application
Please Update `<path/to/code/page-view-counts>` to the path that the code is cloned to.

```
docker run -it -v <path/to/code/page-view-counts>/page-view-counts/.ivy:/home/jovyan/.ivy2 -v <path/to/code/page-view-counts>/page-view-counts:/home/jovyan/page_count page-count
```

# Run unit tests
```
docker run -it -v <path/to/code/page-view-counts>/page-view-counts/.ivy:/home/jovyan/.ivy2 page-count python -m pytest tests
```

# TODO
* Handle exceptions better
