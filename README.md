# bigframes
A solution using BigQuery - BigFrames to fetch London cycle data, process it using bigframes and output some results

## Design considerations
* We use the public BigQuery dataset here, stored in `bigquery-public-data.london_bicycles.cycle_hire`
* Bigframes uses Remote functions to process data in parallel. We use the `map` function to process each row
* Bigframes can map some of the simple transformations to run natively in BigQuery
* Bigframes can dynamically create Remote functions using the 

## Limitations & mitigations
* Bigframes cannot change configuration of remote functions, this is [hard coded](https://github.com/googleapis/python-bigquery-dataframes/blob/main/bigframes/remote_function.py#L404), however, it can use existing Remote functions instead
* Bigframes' dynamically created Remote functions cannot be easily re-used. Any code changes will result in a new function being deployed 
* Bigframes' dynamically created Remote functions has ingress rules open to the internet


## Thanks to
* [Example code from Google Cloud](https://github.com/googleapis/python-bigquery-dataframes/blob/main/samples/snippets/remote_function.py)