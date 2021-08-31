def read_stream(schema, spark, input_format, input_path, read_options={}):
    """
    Function to read spark structured streaming data.

    :param schema: Schema of the data being streamed. (required)
    :param spark: Spark Session. (required)
    :param input_format: Format of the data being streamed. (required)
    :param input_path: Input location for streaming.Must be a folder. (required)
    :param read_options: Map() of any extra options for spark read. (optional, default: {})
    """
    query = (
        spark.readStream.format(input_format)
        .options(**read_options)
    )
    if input_format != "delta":
        query = query.schema(schema)
    return query.load(input_path)
    

def write_stream(output_df, checkpoint=None, func=None, output_sink="console", query_name="query_result"):
    """
    Function to write spark structured streaming data.

    :param output_df: Output dataframe. Must be a streaming source. (required)
    :param checkpoint: Location where checkpoint data can be store. (optional, default: None)
    :param func: Function to be implemented in foreachBatch. (optional, default: None)
    :param output_sink: Output for write Stream like console, parquet etc (optional, default: console)
    :param query_name: Name to the query being executed. Relevant only when output_sink=Memory (optional, default: query_result)
    """
    query = (
        output_df.writeStream
        .format(output_sink)
        # .outputMode("complete")
        .trigger(once=True)
    )
    if output_sink == "memory": 
        query = (
            query
            .queryName(query_name)
        )
    if checkpoint:
        query = query.option("checkpointLocation", checkpoint)
    if func:
        query = query.foreachBatch(func)
    if output_sink=="console":
        query = query.outputMode("update")

    return query.start()