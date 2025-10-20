# anchovies
###### a lightweight ETL library
An "anchovy" is a single executor within a larger data pipeline. Unlike in other ETL orchestration tools, an anchovy has small scope, **generally only focusing on downloading data (as-is) and uploading data (as-is).**
An anchovy runs as a task (start-stop) or a persistent service. Anchovies communicate with each other in order to build a "school" of anchovies that will deliver your entire ETL pipeline.
In order to circumvent complex orchestration and overhead, anchovies relies on timeouts and data limits in order to achieve exclusive or controlled execution of your ETL pipelines.


## Getting started
Install anchovies from git:
```shell
$ pip install "git+https://github.com/raleegh/anchovies.git[sftp]"
# this is installing the sftp "plugin" as well
```
Configure a downloader anchovy by writing a Configuration Document:
```yaml
# ./config.yaml
tbls: 
  sales: 
    # sftp plugin settings
    path: /sales/*.tsv
    delimiter: /t
    # general settings
    autodiscover: true # future functionality
    set_by: order_num
    sort_by: -trans_num, line_item_num, -updated_at
```
Start the integration as a task to integrate data from the SFTP: 
```bash
$ export ACHVY_CNXN__URI='sftp://user:password@example.com:22'
$ export ANCHY_CNXN__PPK_PATH='/path/to/keyfile'
$ anchovy start -op sftp.CsvDownloader --id sales_sftp
```

## Table of contents
* [Concepts](#concepts)
    * [Downloaders & Uploaders](#downloaders--uploaders)
        * [Config file](#config-file)
        * [Task & Service execution](#task--service-execution)
    * [Context](#context)
    * [Datastore](#datastore)
    * [Connections](#connections)
    * [Plugins & customization](#plugins--customization)
        * [Building a custom downloader](#building-a-custom-downloader)
        * [Source & sink](#source--sink)
        * [Runtime task events](#runtime-task-events)
        * [Dynamic table discovery](#dynamic-table-discovery)
        * [Checkpoints](#checkpoints)
* [Roadmap](#roadmap)
* [Appendix](#appendix)
    * [Global Configurations](#global-configurations)
    * [Table Configurations](#table-configurations)
    * [Dataset Variables](#dataset-variables)
    * [Task log attributes](#task-log-attributes)
    * [Command line interface](#command-line-interface)


## Concepts
### Downloaders & uploaders
A downloader is the most common type of anchovy. This anchovy downloads data from a connection and stores it in file system or datalake. As the downloader reads data, it should set up data to be uploaded to a datastore via an uploader.
A downloader loops over "tasks" defined for the downloader either once or continuously. Tasks include: 
* startup & shutdown tasks
* batch open & close tasks
* a task for each table from the config file
* error tasks

An uploader is nearly identical from a downloader, but it will focus on reading data output from a downloader and uploading it to some datatsore (usually a SQL Database).

#### Config file
For some downloaders, metadata may not be "discoverable". In these cases, you must configure a "config file". This is usually a `config.yaml` in the current working directory, but you can also use the `-c/--config` argument to override this.
**Even when metadata is discoverable, you may wish to use a config file in order to control the primary keys/alternate keys used by uploaders.** Any properties set in a config file will be passed to all anchovies using the same tables.
See more at [Table configurations](#table-configurations).

#### Task & Service execution
The default method of execution for a downloader is "task". This intended for batch workflows with slow refresh rates. Once all tasks in the downloader are completed, the downloader will exit. 
The task executor timeline looks like: 
1. startup tasks
2. batch open tasks
3. task for each table
4. batch close tasks
5. shutdown tasks

Anchovies don't use a scheduler instance to control executions. Instead, anchovies rely on primitive external scheduling such as `crontab` or AWS EventBridge Scheduler in the cloud. When using task execution, this could potentially create a problem where the same anchovy is double executed. In order to get around this, you may want to set an execution timeout via `ACHVY_EXECUTION_TIMEOUT`.
If you're writing a custom downloader, make sure to take this behavior into account. If your downloader needs a longer time to execute than provided, you may want to raise a specific exception in order to make it clear this happened. A timeout caused by this setting will use the `AnchovyExecutionTimeout` exception. This also means you may want to consider limiting the maximum amount of data a single "batch" can integrate such as partitioning by date or some other natural key.

Optionally, an anchovy can be started as a persistent service or daemon. Use the `-S/--service` flag to enable this behavior. The service executor is intended for anchovies with fast refresh rate.
The service executor timeline looks like: 
1. startup tasks
2. batch open tasks
3. task for each table
5. batch close tasks
6. **loop back to step 2**

### Context
When an anchovy executes, the entire anchovy is wrapped within "context". This is intended to help develop & maintain anchovies over time. There are two levels of context, each related to the execution model: 
* Session contains the anchovy id & user
    * **id** - a text identifier for the anchovy. by default, this will be the host & port of the executing machine. **in most cases, you should provide a human identifiable name for the anchovy**. this is set via `ACHVY_ID` or `-i/--id`
    * **user** - a user in anchovies is the execution context or developer name. To change the user, change the user environment variable (`ACHVY_USER`) or use the `-u/--user` flag.
        * When deploying to production, set the user to "prod" or use the `--prod` flag. By default, the "prod" user tag will still be associated to all objects.
* Batch contains orchestration information

For example, when using `--user tasytuna`, the datastore will be built with the following structure: 
```
$ACHVY_HOME/tasytuna/<anchovy id>/
    - $tables/
    - $checkpoints/
    - $task_logs/
    - data/
```

Developers can retrieve any level of context at any time via the `context()`, `session()`, and `batch()` magics.

### Datastore
As anchovies download data, they need to record "metadata". This metadata comes in the following forms:
* table metadata (/$tables)
* checkpoint metadata (/$checkpoints)
* task metadata (/$task_logs)
* results (/$results)

By default, your anchovies will build a datastore in the local filesystem with this information. This is good for when anchovies run locally and re-use the same execution environment, such as in local development.

<!-- 
### Tasks & task logs
As you grow your anchovy school, you may find you want options for analyzing & monitoring anchovy execution. Since anchovies are intended to be used massively distributed, this can be tough. All anchovies collect "task logs" for executed tasks as they run (see [Task Executor](#task-executor)).
You can configure the task logs to be stored in a single hive-like formatted table within your metastore rather than the default storage behavior. In order to enable this, change the Task Metastore via environment variable: 
```bash
export ACHVY_TASK_METASTORE_CLASS='HiveTaskMetastore'
```
This will instruct the Metastore to dump all task logs in the same location, partitioned by Anchovy User & ID: 
```
$ACHVY_HOME/$task_logs/anchovy_user=<user>/
    - anchovy_id=<id>/
        - <batch id>.<timestamp>.<rand>.json
        - <batch id>.<timestamp>.<rand>.json
    - anchovy_id=<id2>/
        ...
``` -->


### Connections
Anchovies use Environment Variables in order to build "Connections". A Connection is an object representing an external system such as datastore or Object Storage or API. Using Environment Variables is preferred here in order to keep anchovies lightweight (e.g. no external database is required).
Anchovies are extendable and customizeable via [plugins](#plugins). Each plugin will provide new connections, downloaders, and uploaders. The plugin should specify what connections it adds and which are required for downloaders and uploaders.
During the startup task, connections will be built and added to context. Connection Variables using the `ACHY_CNXN__CNXN_ID__*` format build a "named" connection.
<!-- To see more information about plugin connections, use the `connector` command:
```bash
>>> anchovies connector <<connector name>>
    ...
``` -->
In your code, you only need annotate a connection (or the default property) to build a connection:
```python
class MyDownloader(Downloader): 
    soap: SoapApiConnection # named connection
    web_api=ConnectionMemo('web', WebApiConnection) # named connection style 2
```

### Plugins & customization
A plugin is a package built for the `anchovies.plugins` namespace package. Here is an example: 
```python
# anchovies/plugins/myplugin.py
class CustomDownloader(Downloader): 
    ...

class CustomUploader(Downloader): 
    ...
... more plugin code ...
```

#### Core plugins
* sql - uses [dataset](https://github.com/pudo/dataset) to run SQL Database uploads
* azblob - provides a storage backend for Azure Blob
* sftp - adds a CsvDownloader for SFTP connections

#### Building a custom downloader
In order to retrieve data, build a new Downloader instance. By default, all downloaders route all "sinks" to the default sink, which writes data to the default buffer (this can be overwritten easily)
```python
from anchovies.sdk import Downloader, source, sink

class CustomDownloader(Downloader): 
    @source('sales')
    def download_sales(self, **kwds): 
        for sale in self.cnxn.get_sales(): 
            yield sale

    @source('sales_modifiers')
    @sink('sales')
    def sink_sales_modifiers(self, stream, **kwds): 
        ...
```

#### Source & sink
Within a downloader, you can use `@source/@sink` decorators to declare sources & sinks. _Once a method is decorated, `@source/@sink` will patch-in all common arguments for sources & sinks, so make sure to use `**kwds` to receive all arguments._
A **source** is a generator providing one row at a time for a table(s). A **sink** is a function or generator receiving one row at a time for a table(s). 
You can route all data to a source/sink by not providing the `table` argument. Here are examples of both: 
```python
class MyDownloader(Downloader): 
    @source()
    def source_all(self, table: str, **kwds): 
        ...
    
    @sink()
    def sink_all(self, stream, table, **kwds): 
        ...
```
By default, `@source/@sink` make their calls in a live chain. You may wish, however, to perform operations in "batches" over `@source/@sink`, in which case, use the `cache` argument, which will use Python's `pickle` to dump data to a temporary file and resume processing later.
Users can enable (and disable) certain tables with the `--enable/--disable` options. Based on the desired tables, all connected required sources will be run.

#### Runtime task events
You can also decorate with `@on_task` to add callables to specific task-phases during execution.
```python
class MyDownloader(Downloader): 
    @on_task('xopen')
    def connect_to_api(self): 
        es = self.exit_stack = ExitStack()
        es.open()
        self.engine = es.enter_context(self.connection)
        # just an example of opening a "disconnected" Connection
```
Here is a list of task phases available: 
* startup
* xopen ("batch open")
* xclose ("batch close")
* shutdown
* error
    * xclose & shutdown will always be called as the anchovy process exits, but error will **only** be called if an unhandled exception stops processing

#### Dynamic table discovery
In some cases, such as when we are integrating a OLEDB datastore or heavily-typed API, we may have an ability to DESCRIBE and LIST all tables in the datastore. If this is the case, rather than relying on a `config.yaml` document, we can use dynamic table discovery.
In order to configure this behavior, we need to write and configure a `discover_tbls()` method on our downloader.
Here is an example of what this looks like: 
```python
class DynamicDiscoveryDownloader(Downloader): 
    def discover_tbls(self) -> list[Tbl]: 
        for raw_table in self.connection.show_tables(): 
            tbl = Tbl(... populate with info from raw_table ...)
            yield tbl
```
This method should always provide ALL tables in the associated datastore that are available for integration. Developers should use the `batch().tbls` property to retrieve "active" tables. Additionally, because of source/sink, the `Downloader.sources` property will contain the names of all the applied sources based on source/sink.

#### Checkpoints
As your custom downloader reads data, it should record the state of the integration & tables. This can be done with `Checkpoint`. You can use the magic `context()` method to retrieve the `Checkpoint` associated with the current context. At the end of each `Batch`, `Checkpoint` will be flushed to the datastore. In either task or service execution, you should checkpoint as frequently as possible. The reason is slightly different for each use case: 
* in task execution, the user may limit the execution time with `ACHVY_EXECUTION_TIMEOUT`, so your downloader could shutdown at any time due to this
* in service execution, the service will "crash" if an unhandled exception occurs. keeping your checkpoint up-to-date as you download will ensure that if the service does crash, the state is recorded correctly

Here is an example of recording checkpoints on a paginated API (wrapped by a Client): 
```python
class MyDownloader(Downloader): 
    @source('sales'): 
    def download_sales(self, **kwds)
        checkpoint = context().checkpoint('sales.last_cursor')
        for page in self.cnxn.get_sales(checkpoint.get()): 
            yield from page
            checkpoint.set(page.cursor)
```


## Roadmap
* source/sink caching
* `autodiscover` schema option
* live anchovy communication
* command line enhancements
    * `anchovy school`
    * `anchovy checkpoint`
    * `anchovy plugin`
* enhanced caching & control for service executor anchovies
    * cache refresh rate for things like `discover_tbls()`
* session & batch customization
* parallel execution
    * threading within anchovy
    * splits to workers based on `--worker-num/--workers`
* locking mechanism
* dbt integration
    * add dbt as a Downloader and create tables for:
        * seed
        * run
        * test
        * snapshot
        * docs
    * add a command `anchovy school run --plan <<plan file>>`
        * execute all commands in the "plan file"
        * for each command, create a results.json
        * on exit, upload files to datastore: 
            ```
            $RESULTS_HOME/user/
                anchovy1_results.json
                anchovy2_results.json
                anchovy2_dbt_docs.html
            ```
* option to allow failures in table execution
* streaming buffers & datastore such as: 
    * azure event hubs
    * aws kinesis
    * kafka


## Appendix
### Global Configurations
Almost configs can be configured via CLI option, ENV Variable, and `config.yaml`. Options are shown for CLI, ENV, and then `config.yaml` property name (would go in `config:` map).
* `--id` / `ACHVY_ID` / `config.id` - the anchovy identifier **heavily recommended**
* `--user` / `ACHVY_USER` / `config.user` - the anchovy user (defaults to host profile name)
* `--config` - YAML or JSON data for the `config.yaml` object **CLI only**
* `--config-file` - YAML or JSON file path for the `config.yaml` object **CLI only**
* `-op` / `ACHVY_OPERATOR_CLS` / `config.operator_cls` - the desired operator class to run
* `-S` / `ACHVY_IS_TASK_EXECUTOR` / `config.is_task_executor` - toggle the Service execution mode (defaulting to task execution mode)
* `-t` / `ACHVY_EXECUTION_TIMEOUT` / `config.execution_timeout` - the timeout for a single batch
* `-U` / `ACHVY_UPSTREAM` / `config.upstream` - a list of upstream anchovy ids. in the CLI, this can be used multiple times (e.g. `-U anch1 -U anch2`).
* `-e` / `ACHVY_ENABLED` / `config.enabled` - a list of enabled tables. this will restrict the operator to ONLY use these tables. in the CLI, this can be used multiple times.
* `-d` / `ACHVY_DISABLED` / `config.disabled` - a list of disabled tables. this will restrict the operator to SKIP these tables, overriding the `enabled` setting. in the CLI, this can be used multiple times.
* `--datastore` / `ACHVY_DATASTORE` / `config.datastore` - the path/connection uri for the configured datastore
    * `--checkpoint-cls` / `ACHVY_CHECKPOINT_CLS` / `config.checpoint_cls` - the runtime checkpoint class to use. use an anchovies plugins import path.
    * `--task-store-cls` / `ACHVY_TASK_STORE_CLS` / `config.task_store_cls` - the runtime class to use for the task store. use an anchovies plugins import path.
    * `--tbl-store-cls` / `ACHVY_TBL_STORE_CLS` / `config.tbl_store_cls` - the runtime tbl store class to use. use an anchovies plugins import path.
* `--data-buffer-cls` / `ACHVY_DATA_BUFFER_CLS` / `config.data_buffer_cls` - the default data buffer used by downloaders/uploader. this is one of the best ways to customize how anchovies interacts with your data lake. if you don't like the default data format, change this. use an anchovies plugins import path.
* `ACHVY_CNXN__{connection id}__{attribute}` / `cnxns.{id}.{attribute}` - connection properties
    * **not supported in config.yaml currently**

### Table Configurations
These properties can be set under `tbls:` in the yaml file
* **tbls.{name}.columns** - when not using autodiscovery, a map of column and data type
    * currently, only python types are supported. see the following types: 
        * str (string types)
        * int (integer types)
        * float (numeric/decimal types)
        * bool (boolean types)
        * datetime, date, time from python datetime (datetime/time constructs)
        * bytes (binary data)
        * object (for complex/nested types)
* **tbls.{name}.unique_by** - a comma separated list of column names by which to consider a table's rows unique AKA the primary key or alternate key
* **tbls.{name}.set_by** - in some cases, you may wish to write data in "sets" rather than using a primary key. for example, if we receive all order line items for an order together, you can use the "order_id" as the alternate key, which will help keep the set together and correct
* **tbls.{name}.sort_by** - among all rows, provide a sort order. when used with `set_by`, this should form a unique key for all records in the data stream
    * prefix a `sort_by` column with "-" to specify descending order
<!-- * **tables.{name}.logical_timestamp** - among all rows, the column name of a timestamp which should be used to "logically" monitor the position of the integration -->
* **tbls.{name}.{plugin specific}** - for plugins, provide custom properties

### Dataset variables
If you are using the `sql` plugin, anchovies uses a heavily modified `dataset` library under the hood. These configurations may be relevant: 
* `DATASET_PRIMARY_LENGTH` - for databases that rely on string length (e.g. postgres), this is an integer that will be used to create columns in the primary key. defaults to 40.
* `DATASET_DEFAULT_LENGTH` - similar to `DATASET_PRIMARY_LENGTH`, this will be used to create non-key string columns. defaults to 255.
* `DATASET_CHUNKSIZE` - for uploaders using SQLAlchemy import, this is the number of records that will be moved in each batch. corresponds to the system Buffer Size for the default. can be disabled with -1, meaning all records get "streamed" in one go.
* `DATASET_SAMPLE_SIZE` - optionally, only sample a small number of records for the dynamic data discovery. recommended to use when `DATASET_CHUNKSIZE=-1` because otherwise, all data will be read into memory.
* `DATASET_DATA_RETENTION_SECONDS` - the default behavior is to use an SCD-type merge with columns `_seq`/`_del`. once a record is "expired" via `_del`, it will only be retained for this amount of seconds. defaults to -1 or never expire.

### Task log attributes
* `anchovy_id` - the anchovy id from the caller/invoker
* `anchovy_user` - the uid of the caller/invoker
* `session_id` - the session id associated with the execution. by default, roughly corresponds to a timestamp.
* `batch_id` - the batch id associated with the execution. by default, an integer sequence number.
* `thread_id` - the thread identifier of the execution.
* `operator` - the operator class name
* `task_type` - STARTUP, XOPEN, TABLE, XCLOSE, ERROR, SHUTDOWN
* `duration` - the duration of the task in milliseconds
* `timestamp` - the timestamp at which the task completed
* `status` - OK / ERR
* `tbl` - if the `task_type` is TBL, then this will be the associated table name
* `data` - data appended to the task during execution. this is a JSON object.

### Command line interface
Once installed, use the `anchovy` command to access the CLI. Here is a quick cheat-sheet of commands:
* `anchovy start` - create a new anchovy task/service
* `anchovy healthcheck` - run a healthcheck
<!-- * `anchovy school` - describe all anchovies in the school, if a Metastore is registered
    * set `ACHVY_METASTORE` to connect to a school
* `anchovy checkpoint` - set & get checkpoints
    * use `anchovy checkpoint -i <id> -d` to delete all checkpoints, forcing a full repull
* `anchovy plugin` - get info about plugins -->
