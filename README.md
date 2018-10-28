# Crawler
Shaman - multiprocessing tool for parallel work with data
Description
Main idea of shaman is to create pipelines (so called workers) of operations (so called stages) and apply it to data. Here is main principles:

workers are composed from stages
there are 3 types of stages - input, output and common
stages order in worker matters
workers can have only one input stage and it should be first
workers configuration (together with each stage configuration) is stored in one configuration file
This set of principles allow to use shaman for different (sometimes unexpected) scenarios:

web-grabber - start in no deamon mode, read urls from stdin, download it with grab, parse page attributes, put some attributes to mongodb, some attributes append to files as json or simple text, some put to kafka
data processor - start in no daemon mode, scan mongodb, apply some operations on documents fields, write it back
daemon data processor - start in daemon mode, read kafka topic, operate stages on kafka messages, write results back to other kafka topic(s)
...
Features
Simplicity - one configuration for worker stages and main attributes (number of parallel workers, logging directories, etc.)
Multiprocessing - shaman can spawn a pool of homogeneous workers and run them in parallel
Daemonizing - shaman could be run as daemon
Logging - shaman provides distinct logging file for main module and each worker
Examples
####Example 1 - download list of urls, contained in textfile with '\n' separator

Let's imagine that we have a list of urls in file 'urls.txt' and we want to download them. Some urls would be successfully downloaded and we want to put results in one directory, others would be unavailable and we want to put failed results to another directory. Also we want to do it parallel with 32 proccesses:

cat urls.txt | shaman -i -c etc/crawler.config
Pay attention to shaman arguments, '-i' stands for "read input from stdin, line by line", and '-c' is for "get this config file".

Right after starting this command you'll see each url from list printed in your terminal. Successful results would be saved in '/tmp/shaman/data/downloaded/worker_[N]' files and failed results would be in '/tmp/shaman/data/failed/worker_[N]' files.

Let's dig a little more in config file to understand what just happened.

[GENERAL]
basepath = /tmp/shaman
num_workers = 32
logfile_path = /tmp/shaman/shaman_daemon.log
pidfile_path = /tmp/shaman

workers_logging_dir = /tmp/shaman
worker_prefix = worker_
stages_dir = shamanapp/analyzers/input_stages;shamanapp/analyzers/output_stages;shamanapp/analyzers/general_stages

[GRAB]
connect_timeout = 3

[GRAPHITE]
enabled = False
global_key = shaman
host = localhost
port = 2411

[STAGES]
stdin_reader_stage = 'classname':'StdinReaderStage'
download_stage = 'classname':'DownloadStage'
file_output_stage_failed = 'classname':'FileOutputStage','python_class_filename':'file_output_stage'
file_output_stage_downloaded = 'classname':'FileOutputStage','python_class_filename':'file_output_stage'

[stdin_reader_stage]
order = 0
owner = testing
comment = ''

[download_stage]
connect_timeout = 3
download_timeout = 3
order = 10

[file_output_stage_failed]
order = 20
out_dir = /tmp/shaman/data/failed
fields_to_print = results

[file_output_stage_downloaded]
order = 25
out_dir = /tmp/shaman/data/downloaded
fields_to_print = output_dict

[message_printer]
order = 35
fields_to_print = url
Every stage has it's own configuration block. It looks like:

[download_stage]  
connect_timeout = 3  
download_timeout = 3  
order = 10  
The only non-optional parameter is 'order'. It should be unique. Other parameters are optional and depends on a particular stage. For instance, in case of 'download_stage', where we use python grab to download a page there are two parameters:

connect_timeout (maximum time to wait a server response)
download_timeout (maximum time to download a page) In this case we redefined those parameters.
All stages are split to different types (input, output, common). It is possible to create your own stage, that does something with the results from other stages. The basic workflow: 1. Worker creates a Message object (empty container) 2. It reads an input data (from kafka, stding or other) 3. Message object goes through a set of stages (in a particular order) 4. Every stage acqure some attribute from a Message object and does something with it. Then, a stage puts it's result (as an attribute) to a Message. 5. If no exception occured, after the last stage is done, worker starts to work on a new input data.

[STAGES]
stdin_reader_stage = 'classname':'StdinReaderStage'
download_stage = 'classname':'DownloadStage'
file_output_stage_failed = 'classname':'FileOutputStage','python_class_filename':'file_output_stage'
file_output_stage_downloaded = 'classname':'FileOutputStage','python_class_filename':'file_output_stage'
The usual format is the following: <name_of_python_module> = 'classname':<name_of_python_class>

In the case of two different stages with the one pythone module config will be: <custom_name_of_stage> = 'classname':<name_of_python_class>,'python_class_filename':<name_of_python_module>

Link: http://shaman.readthedocs.io/en/latest/
