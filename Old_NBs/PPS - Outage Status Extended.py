# Databricks notebook source
"/EDA/Analyst/Framework/Secrets-Databricks-Cache"

# COMMAND ----------

# MAGIC %md
# MAGIC # PPS Outage Analysis
# MAGIC
# MAGIC ## Purpose
# MAGIC To create a timeline of outage status per meter and job by applying business rules that mirror the PPS outage system to analyze the number of meters in "Don't Know" status from job close
# MAGIC
# MAGIC ## Deliverable
# MAGIC Workbook with widgets that can produce the analysis for a given date range.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Enter a file name and ADLS folder path (with the last "/" on the folder path)
# MAGIC 2. Enter a start/stop date in the yyyy-mm-dd HH:MM:SS (keep leading 0s on single digit months, days, hours, minutes, seconds) Time is in EST
# MAGIC 3. Select or confirm  dte-cu-prod-premisepowerstatus-cluster-001 is the selected cluster and is running
# MAGIC 4. Run all and the file will be written to the specified location in ADLS
# MAGIC 5. For single meter numbers, go to the section "* Run single profiles" and uncomment and run the workbook up to that point
# MAGIC 6. To customize interval range, got to section "Set reporting intervals" and update teh 
# MAGIC
# MAGIC ## Steps
# MAGIC 1. Prepare data by pulling in needed tables and summarizing the voltage reads
# MAGIC 2. Create time intervals on the meters/outages in order to begin applying business rules
# MAGIC 3. Apply business rules for around if power status is "Yes", "No", "Don't Know"
# MAGIC 4. Apply the 95% power rule for impacted meters
# MAGIC 5. Group up into intervals to provide summarized data
# MAGIC 6. Write to file
# MAGIC
# MAGIC ## Source Tables
# MAGIC
# MAGIC #### edw.location_outage_history
# MAGIC InService jobs tied to premise. Must filter for date intervals (user input) and outage job TYCOD outage codes (provided by DTE)
# MAGIC
# MAGIC #### edw.cispersl2
# MAGIC Used here as the link table between premise and meter number
# MAGIC
# MAGIC #### edw.ami_voltage_intervals
# MAGIC Five minute voltage reads for each meter. Currently using the eda_creation_date_localtime and taking the voltage from lastest 5 minute read prior to the eda_creation_date_localtime
# MAGIC
# MAGIC #### edw.nmnlvltg
# MAGIC Contains the nominal voltage for each meter
# MAGIC
# MAGIC #### edw.ami_elc_meter_events
# MAGIC Different events, but for this analysis we only care about primary power up events
# MAGIC
# MAGIC #### edw.outcall
# MAGIC Outage call table

# COMMAND ----------

dbutils.widgets.text("start_date", "", "Start Date")
dbutils.widgets.text("stop_date", "", "Stop Date")
dbutils.widgets.text("path", "", "Folder Path")
dbutils.widgets.text("name", "", "File Name")

# COMMAND ----------

start_date = dbutils.widgets.get("start_date")
stop_date = dbutils.widgets.get("stop_date")
path = dbutils.widgets.get("path")
name = dbutils.widgets.get("name")

# COMMAND ----------

#start_date = "2021-10-11 09:24:00"
#stop_date = "2021-10-13 12:24:02"

# COMMAND ----------

# MAGIC %md
# MAGIC # A) Prep data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read in functions

# COMMAND ----------

from datetime import datetime, timedelta
import itertools
from pyspark.sql.functions import col, substring, row_number, when, lit, to_timestamp, length, lag, greatest, last, sum, expr, min, unix_timestamp, monotonically_increasing_id
from pyspark.sql.window import Window

#spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

date_fmt = "%Y-%m-%d %H:%M:%S"
start_dt = datetime.strptime(start_date, date_fmt) + timedelta(hours=-10)
stop_dt = datetime.strptime(stop_date, date_fmt) + timedelta(hours=72)

ami_start_part_key = int(f"{start_dt.year}{'{:02d}'.format(start_dt.month)}{'{:02d}'.format(start_dt.day)}")
ami_stop_part_key = int(f"{stop_dt.year}{'{:02d}'.format(stop_dt.month)}{'{:02d}'.format(stop_dt.day)}")

date_range_key = list(map(lambda x: str(x), range(ami_start_part_key, ami_stop_part_key + 1)))
date_ym_key = list(set(map(lambda x: x[0:6], date_range_key)))

p_read_time_lt_query = ",".join(date_range_key)
p_read_time_lt_ym_query = ",".join(date_ym_key)

# COMMAND ----------

# MAGIC %md
# MAGIC #### List of outage TYCOD values

# COMMAND ----------

outage_types = ['CP','FUSE','ISO','OHCKT','OHTRANS','RECLO','UGCKT','UGPSC','UGTRANS', 'CUTCOND','OHDS','OHSWC','UGDS','UGFUSE', 'SDXL','ONELEG','XCURR','TEMPOHDS','TEMPFUSE']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build storm timeline intervals

# COMMAND ----------

rng = itertools.count(0,1)
  
date_rng = map(lambda x: {"interval": f"+{x} hours", "time_step": x, "end_time_step": x+1, "interval_tm_start": start_dt + timedelta(hours=x),"interval_tm_stop": start_dt + timedelta(hours=(x+1))}, rng)
interval_data = itertools.takewhile(lambda x: x["interval_tm_start"] <= stop_dt, date_rng)

interval_df = spark.createDataFrame(interval_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build job timeline intervals

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set reporting intervals
# MAGIC
# MAGIC ##### To update
# MAGIC
# MAGIC Fill in the hour intervals below in "hour_intervals". The interval is the hour length of the interval [2, 3] means the first interval will be up to hour 2 and the second will be 2-5 (length 3). A catch all interval will be added (in the example case 5+)

# COMMAND ----------

hour_intervals = [2] * 48


import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

a = 2
class conversion():
  #This is the UDF. 
  @staticmethod
  def min_func(a):
    return a*60
  @staticmethod
  def hour_func(y):
    return y*60*60
  #This is the function that registers the UDF, 
  def input(self, a, target):
      interval_data =  [{'job_interval':  '0' + '-' +  str(a) +  ' min',  'job_time_step': 0, 'job_end_time_step': conversion.min_func(a)}, {'job_interval': str(a)+' min' + '-' + str(target[0]) + ' hour', 'job_time_step': conversion.min_func(a), 'job_end_time_step': conversion.hour_func(target[0])}] 
      for i in range(len(target)):
          if (i != 0):
              lst = []
              for n in range(i):
                  lst.append(target[n])
              prev = 0
              for j in lst:
                   prev = j + prev
              nst = []
              for n in range(i+1):
                  nst.append(target[n])
              later = 0
              for h in nst:
                  later = h + later
              interval_data = interval_data + [ {'job_interval': str(prev) + ' hour' + '-' +  str(later) + ' hour', 'job_time_step': conversion().hour_func(prev),  'job_end_time_step': conversion().hour_func(later)}]
      interval_data = interval_data +  [{'job_interval': str(later)+ ' hours+', 'job_time_step' : conversion().hour_func(later), 'job_end_time_step': 999999}] 
      return interval_data

# COMMAND ----------

interval_data = conversion().input(a, hour_intervals)
job_interval_data = spark.createDataFrame(interval_data)

# COMMAND ----------

# last_interval = 96
# hour_step = 2
# rng = itertools.count(0, hour_step)

# def create_job_interval(hour: int):
#     return {"job_interval": f"{hour} - {hour+hour_step} hours", 
#             "job_time_step": hour * 3600,
#             "job_end_time_step": (hour+hour_step) * 3600}

# date_rng = map(create_job_interval, rng)

# interval_data = [*itertools.takewhile(lambda x: x["job_end_time_step"] <= last_interval * 3600, date_rng)]
# final_interval = [{"job_interval": f"{last_interval}+ hours", 
#                   "job_time_step": last_interval * 3600,
#                   "job_end_time_step": (last_interval * 100) * 3600}]

# job_interval_data = spark.createDataFrame(interval_data + final_interval)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter outage interval for time period and outage events and convert to list of events

# COMMAND ----------

import pyspark.sql.functions as f
col = f.col

#filtered_outage_intervals_df = spark.sql("""select event_number, premise, tycod, out_dt_tm, on_dt_tm 
filtered_outage_intervals_df2 = spark.sql("""select *
                                            from edw.location_outage_history""") \
  .filter(col("tycod").isin(outage_types) & \
          (col("OUT_DT_TM") >= start_date) & \
          (col("ON_DT_TM") <= stop_date) & \
          (col("EVENT_STATUS") != 'C') )


# COMMAND ----------

from pyspark.sql.functions import *

filtered_outage_intervals_df3 = filtered_outage_intervals_df2.withColumn('from_timestamp', to_timestamp(col('OUT_DT_TM')))\
  .withColumn('end_timestamp', to_timestamp(col('ON_DT_TM')))\
  .withColumn('DiffInSeconds', col('end_timestamp').cast('long') - col('from_timestamp').cast('long')).drop('from_timestamp', 'end_timestamp').filter(col('DiffInSeconds') >= 300).drop('DiffInSeconds')

#filtered_outage_intervals_df.select('OUT_DT_TM', 'ON_DT_TM').show()

# COMMAND ----------

filtered_outage_intervals_df = filtered_outage_intervals_df3.select('event_number', 'premise', 'tycod', 'out_dt_tm', 'on_dt_tm')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge with cispersl2 to get premise ID

# COMMAND ----------

outage_df = filtered_outage_intervals_df.join(spark.sql("""select METER_NUM as meter_number, premise from edw.cispersl2
                                                            where DISCON_FOR_NON_PAY='A' and AMI_ENABLED = 'T'"""),\
                                  "premise",\
                                  "inner") \
  .withColumnRenamed("out_dt_tm", "job_out_dt_tm") \
  .withColumnRenamed("on_dt_tm", "job_on_dt_tm") \
  .withColumnRenamed("meter_number", "job_meter_number") \
  .withColumnRenamed("premise", "job_premise") 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create distinct list of meters impacted by outage events

# COMMAND ----------

filtered_premise_meter_df = outage_df \
  .select("job_premise", "job_meter_number") \
  .distinct() \
  .withColumnRenamed("job_premise", "premise") \
  .withColumnRenamed("job_meter_number", "meter_number")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create counts by outage job to select largest outage jobs for tie break when jobs overlap
# MAGIC
# MAGIC Lower rank indicates a larger job size

# COMMAND ----------

from pyspark.sql.functions import count

window = Window.orderBy(col("meters_in_job").desc())

job_rank = outage_df \
  .groupBy(col("event_number")) \
  .agg(count(col("job_meter_number")).alias("meters_in_job")) \
  .withColumn("job_rank", row_number().over(window)) \
  .drop("meters_in_job")

# COMMAND ----------

outage_premise_df = outage_df \
  .join(job_rank, "event_number", "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pull in relevant AMI voltage reads
# MAGIC 1. Grab AMI reads filtered by local date partition key
# MAGIC 2. Inner join with filtered outage events by premise ID
# MAGIC 3. Keep: meter ID, premise ID, volatage read, read time, read_time local, eda ingestion time
# MAGIC 4. Group by meter, premise, eda ingestion time
# MAGIC 5. Order by max read time and max Vh(a)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select the meters impacted and cut down to leading two digits for partition query

# COMMAND ----------

meter_range = outage_premise_df \
  .select(col("job_meter_number")).rdd \
  .distinct().flatMap(lambda x: x).collect()

meter_range_key = list(map(lambda x: x[-2:], filter(lambda x: x != None, meter_range)))
p_mpartkey_query = "'" + "','".join(meter_range_key) + "'"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create query on voltage intervals
# MAGIC Per discussion we are filtering for Vh(a) and then multiplying by 12 to get an hourly read

# COMMAND ----------

intermediate_df = spark.sql("""select * from edw.ami_voltage_intervals
                                         where p_read_time_lt in ({0}) and
                                              p_mpartkey in ({1}) and
                                               meter_quantity = 'Vh(a)'""".format(p_read_time_lt_query, p_mpartkey_query))

trimmed_volt_reads_df = intermediate_df.join(outage_premise_df, intermediate_df.meter_number == outage_premise_df.job_meter_number, "inner") \
                            .select(col("meter_number"), col("meter_quantity"), col("read_time_localtime"),\
                                    col("read_value"), col("eda_creation_date_localtime")) \
                            .withColumn("full_read_value", col("read_value") * 12)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by meter number, event number, eda creation date and select max read time prior to ingestion time and full read value (this last shouldn't matter) row

# COMMAND ----------

window = Window.partitionBy(col("meter_number"), \
                            col("eda_creation_date_localtime")) \
  .orderBy(col("read_time_localtime").desc())

selected_volt_per_interval_df = trimmed_volt_reads_df \
  .withColumn("row", row_number().over(window)) \
  .filter(col("row") == 1) \
  .select(col("meter_number"),  col("read_time_localtime"), col("full_read_value"), \
          col("eda_creation_date_localtime"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Attach nominal voltage and calculate nominal voltage percent

# COMMAND ----------

nmnl_volt_df = spark.sql("""select METER_NUMBER as meter_number, NMNL_VOLT_A as nmnl_volt from edw.nmnlvltg""")

# COMMAND ----------

volt_per_interval_nominal_df = selected_volt_per_interval_df \
  .join(nmnl_volt_df, "meter_number", "inner") \
  .withColumn("nmnl_volt_pct", col("full_read_value") / col("nmnl_volt")) \
  .withColumnRenamed("meter_number", "meter_number_volt") \
  .drop(col("nmnl_volt"))

# COMMAND ----------

# MAGIC %md
# MAGIC # B) Create intervals, job status and initial power status

# COMMAND ----------

full_meter_intervals = filtered_premise_meter_df \
  .join(interval_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join on jobs and handle overlapping by taking the most populous job if two

# COMMAND ----------

cond_jobs = (col("job_meter_number") == col("meter_number")) & \
             ((col("job_out_dt_tm").between(col("interval_tm_start"), col("interval_tm_stop"))) | \
             (col("job_on_dt_tm").between(col("interval_tm_start"), col("interval_tm_stop"))) | \
             ((col("job_out_dt_tm") <= col("interval_tm_start")) & (col("job_on_dt_tm") >= col("interval_tm_stop"))))
              
raw_outage_intervals = full_meter_intervals \
  .join(outage_premise_df.drop("job_premise"), on=cond_jobs, how="left_outer")


window = Window.partitionBy(raw_outage_intervals.meter_number, col("interval")) \
  .orderBy(col("job_rank"))

selected_outage_intervals = raw_outage_intervals \
  .withColumn("row", row_number().over(window)) \
  .filter(col("row") == 1) \
  .drop("row", "job_rank")

# COMMAND ----------

outage_intervals = selected_outage_intervals \
  .drop("job_meter") \
  .withColumnRenamed("job_out_dt_tm", "out_dt_tm") \
  .withColumnRenamed("job_on_dt_tm", "on_dt_tm") \
  .withColumn("job_status", when(col("on_dt_tm") > col("interval_tm_stop"),  lit("Job Open"))
                           .when(col("on_dt_tm") <= col("interval_tm_stop"), lit("Job Closed"))
                           .otherwise(lit("Job Closed"))) \
  .withColumn("job_start", when(col("out_dt_tm") >= col("interval_tm_start"), lit("Yes"))) \
  .withColumn("power_status", lit(None))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create small interval around job start
# MAGIC
# MAGIC 1. job_start_intervals creates an interval 1 minute around the job inside the starting interval and puts it at 0.2 to keep it in order
# MAGIC 2. job_prior_intervals splits the original starting interval into an interval before the job starts
# MAGIC 3. On the interval right before a job, check that there isn't a prior job that just ended

# COMMAND ----------

check_not_middle_job_window = Window.partitionBy(col("meter_number")).orderBy(col("time_step"))

job_start_intervals = outage_intervals.filter(col("job_start") == "Yes") \
  .withColumn("job_status", lit("Job Open")) \
  .withColumn("interval_tm_start", col("out_dt_tm")) \
  .withColumn("interval_tm_stop", col("out_dt_tm") + expr('INTERVAL 1 minutes')) \
  .withColumn("time_step", col("time_step") + 0.2)

job_prior_intervals = outage_intervals.filter(col("job_start") == "Yes") \
  .withColumn("interval_tm_stop", col("out_dt_tm")) \
  .withColumn("in_between_jobs", lag(col("event_number"), 1).over(check_not_middle_job_window)) \
  .withColumn("job_start", when(col("in_between_jobs").isNull(), lit(None)).otherwise(col("job_start"))) \
  .withColumn("job_status", when(col("in_between_jobs").isNull(), lit("Job Closed")).otherwise(col("job_status")) ) \
  .withColumn("out_dt_tm", when(col("in_between_jobs").isNull(), lit(None)).otherwise(col("out_dt_tm"))) \
  .withColumn("on_dt_tm", when(col("in_between_jobs").isNull(), lit(None)).otherwise(col("on_dt_tm"))) \
  .withColumn("event_number", when(col("in_between_jobs").isNull(), lit(None)).otherwise(col("event_number"))) \
  .withColumn("tycod", when(col("in_between_jobs").isNull(), lit(None)).otherwise(col("tycod"))) \
  .withColumn("job_meter_number", when(col("in_between_jobs").isNull(), lit(None)).otherwise(col("job_meter_number"))) \
  .withColumn("time_step", col("time_step") + 0.1) \
  .drop(col("in_between_jobs"))

job_post_intervals = outage_intervals.filter(col("job_start") == "Yes") \
  .withColumn("interval_tm_start", col("out_dt_tm") + expr('INTERVAL 1 minutes')) \
  .withColumn("job_start", lit(None)) \
  .withColumn("time_step", col("time_step") + 0.3)

without_job_intervals = outage_intervals.filter(col("job_start").isNull())

split_outage_intervals = without_job_intervals \
  .union(job_start_intervals) \
  .union(job_prior_intervals) \
  .union(job_post_intervals)

# COMMAND ----------

job_start_intervals.columns

# COMMAND ----------

job_prior_intervals.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join all indications/events
# MAGIC 1. Join the voltage reads
# MAGIC 2. Join on PPU up events
# MAGIC 3. Join on calls

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) Range join on voltage reads
# MAGIC 1. Join where meter number and interval match
# MAGIC 2. Make sure that we take latest eda date inside interval

# COMMAND ----------

cond_volt = (col("meter_number") == col("meter_number_volt")) & \
             (col("interval_tm_start") <= col("eda_creation_date_localtime")) & \
             (col("interval_tm_stop") > col("eda_creation_date_localtime"))

interval_volt_df = split_outage_intervals \
    .join(volt_per_interval_nominal_df, on=cond_volt, how="left_outer")

window_volt = Window.partitionBy(split_outage_intervals.meter_number, col("time_step")) \
  .orderBy(col("eda_creation_date_localtime").desc())

selected_interval_volt_df = interval_volt_df \
  .withColumn("row", row_number().over(window_volt)) \
  .filter(col("row") == 1) \
  .drop("row", "event_number_volt", "meter_number_volt", "read_time_localtime")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Range join the PPU up events
# MAGIC 1. Join meter number and interval
# MAGIC 2. Make sure to take the latest

# COMMAND ----------

events_df = spark.sql("""select meter_number as meter_number_ppu, event_name, eda_creation_date_localtime as ppu_eda_creation_date_localtime from edw.ami_elc_meter_events
                         where p_event_time_lt_ym in ({0}) and
                               p_mpartkey in ({1}) and
                               event_name = 'Primary Power Up'""".format(p_read_time_lt_ym_query, p_mpartkey_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Take latest PPU inside time interval and join

# COMMAND ----------

cond_event = (col("meter_number") == col("meter_number_ppu")) & \
             (col("interval_tm_start") <= col("ppu_eda_creation_date_localtime")) & \
             (col("interval_tm_stop") > col("ppu_eda_creation_date_localtime"))

interval_volt_ppu_df = selected_interval_volt_df \
    .join(events_df, on=cond_event, how="left_outer")

window = Window.partitionBy(col("meter_number"), col("time_step")) \
  .orderBy(col("ppu_eda_creation_date_localtime").desc())

selected_interval_volt_ppu_df = interval_volt_ppu_df \
  .withColumn("row", row_number().over(window)) \
  .filter(col("row") == 1) \
  .drop("row", "event_number_volt", "meter_number_ppu")


# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Range join on outage call events

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create datetime for outage calls

# COMMAND ----------

outcall_df = spark.sql("""select METER_NUM as meter_number_call, EVNTNUM as event_number_call, CDTS, CALLTYPE from edw.outcall""") \
  .withColumn("CDTS_trim", col("CDTS").substr(lit(0), length(col('CDTS')) - 2)) \
  .withColumn("CDTS_datetime", to_timestamp(col("CDTS_trim"), "yyyyMMddHHmmss")) \
  .drop("CDTS_trim", "CDTS")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Perform the join

# COMMAND ----------

cond_call = (col("meter_number") == col("meter_number_call")) & \
             (col("interval_tm_start") <= col("CDTS_datetime")) & \
             (col("interval_tm_stop") > col("CDTS_datetime"))

interval_volt_ppu_call_df = selected_interval_volt_ppu_df \
    .join(outcall_df, on=cond_call, how="left_outer")

window = Window.partitionBy(col("meter_number"), col("time_step")) \
  .orderBy(col("CDTS_datetime").desc())

selected_interval_volt_ppu_call_df = interval_volt_ppu_call_df \
  .withColumn("row", row_number().over(window)) \
  .filter(col("row") == 1) \
  .drop("row", "event_number_call", "meter_number_call")

# COMMAND ----------

# MAGIC %md
# MAGIC # C) Apply business logic to each meter combo sequentially

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Application of business rules

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update job_status, power_status names
# MAGIC 2. Find the maximum timestamp of events in each interval
# MAGIC 3. Determine which event (voltage read, outage call, PPU - primary power up)
# MAGIC 4. Adding a lag variable to be able to determine when job starts and ends
# MAGIC 5. Mark the discrete events that occur and the power status
# MAGIC 6. Fill in the gaps between jobs
# MAGIC 7. Repeat this process of marking and filling for the Job Open
# MAGIC 8. Calculate the 95% rule at job close 
# MAGIC 9. Repeat the mark and fill process

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Update names, find max timestamp of event, determine which event and add lagging job status variable

# COMMAND ----------

bus_logic_window = Window.partitionBy("meter_number").orderBy(col("time_step").asc())

rename_voltage_df = selected_interval_volt_ppu_call_df \
  .withColumnRenamed('job_status', 'job_status_orig') \
  .withColumnRenamed('power_status', 'power_status_orig') \
  .withColumn('time_occurence', greatest(col("eda_creation_date_localtime"), col("ppu_eda_creation_date_localtime"), col("CDTS_datetime"))) \
  .withColumn("max_event", when(col("eda_creation_date_localtime") == col("time_occurence"), "volt") \
                          .when(col("ppu_eda_creation_date_localtime") == col("time_occurence"), "ppu") \
                          .when(col("CDTS_datetime") == col("time_occurence"), "call")) \
  .withColumn('job_status_over', lag(col("job_status_orig"), 1).over(bus_logic_window)) \
  .withColumn('power_status_over', lag(col("power_status_orig"), 1).over(bus_logic_window)) \
  .withColumn('event_over', lag(col("event_number"), -1).over(bus_logic_window))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mark discrete outage events
# MAGIC
# MAGIC Keep in mind we have to make sure the outage job is not null for calls to keep call "No" response only being updated inside the job window, the lookback will be handled in next step

# COMMAND ----------


discrete_bis_rules = rename_voltage_df.withColumn("power_status_orig", 
            when((col("max_event") == "ppu"), "Yes")
            .when((col("max_event") == "call") & (col("event_number").isNotNull()), "No")
            .when((col("max_event") == "volt") & (col("full_read_value") < 90), "No")
            .when((col("max_event") == "volt") & (col("nmnl_volt_pct") >= 0.92), "Yes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Switch Yes status to Don't know on job open, mark and fill and get lagging call for first open
# MAGIC
# MAGIC We have call information, we can go back at least two records before, taking the first non-null event we can find (that is between 1 and 2 hours prior to the outage job), then we check if it was a call and the job is opening and mark "No"

# COMMAND ----------

full_business_no_95_rule = discrete_bis_rules \
  .withColumn('power_status_orig_compare', last('power_status_orig', ignorenulls=True).over(bus_logic_window.rowsBetween(Window.unboundedPreceding, 0))) \
  .withColumn('lag_max_event', last('max_event', ignorenulls=True).over(bus_logic_window.rowsBetween(-2, 0))) \
  .withColumn("power_status_orig_dk",                 
      when((col("power_status_orig_compare") == "Yes") & (col("job_status_orig") =="Job Open") & (col("job_status_over")=="Job Closed"), "Don't Know")
     .when((col("lag_max_event") == "call") & (col("job_status_orig") =="Job Open") & (col("job_status_over")=="Job Closed"), "No")
     .otherwise(col("power_status_orig"))) \
  .withColumn('final_power_status', last('power_status_orig_dk', ignorenulls=True).over(bus_logic_window)) \
  .drop(col("lag_max_event"))

# COMMAND ----------

# MAGIC %md
# MAGIC # D)  95% rule
# MAGIC
# MAGIC - If status is No and 95%+ of meters impacted by the outage job are Yes and no event happens, the meter power status becomes Don’t Know
# MAGIC
# MAGIC - If status is Don’t Know and <95% of meters impacted by the outage job are Yes and no event happens, the meter power status becomes No
# MAGIC
# MAGIC
# MAGIC ##### Calculate the power status at job close, then mark the locations where power status changes and fill

# COMMAND ----------

job_interval_percentage = full_business_no_95_rule \
  .filter((col("job_status_orig") == "Job Closed") & (col("job_status_over") == "Job Open")) \
  .select(col("meter_number"), col("event_number"), col("final_power_status"), col("job_status_orig")) \
  .withColumn("power_yes_status", when(col("final_power_status") == "Yes", 1).otherwise(0)) \
  .groupBy(col("event_number")) \
  .agg(count(col("meter_number")).alias("meters_in_job"), sum(col("power_yes_status")).alias("power_yes_status")) \
  .withColumn("percent_yes_meters", col("power_yes_status") / col("meters_in_job"))

full_business_95 = full_business_no_95_rule \
  .join(job_interval_percentage, on="event_number", how = "left") \
  .withColumn("power_status_orig_95", when((col("job_status_orig") == "Job Closed") & (col("job_status_over") == "Job Open") & (col("max_event").isNull()),
                                       when((col("final_power_status") == "Don't Know") & (col("percent_yes_meters") < 0.95), "No") \
                                       .when((col("final_power_status") == "No") & (col("percent_yes_meters") >= 0.95), "Yes")) \
                                      .otherwise(col("power_status_orig_dk"))) \
  .withColumn('final_power_status_95', last('power_status_orig_95', ignorenulls=True).over(bus_logic_window)) \
  .drop("job_meter_number", "job_start", "power_status_orig", "CALLTYPE", "event_name", "time_occurence", "job_status_over",
        "power_status_over", "power_status_orig_compare", "final_power_status", "power_status_orig_dk", "meters_in_job", "power_status_orig_95", "power_yes_status")


# COMMAND ----------

# MAGIC %md
# MAGIC # * Run single profiles

# COMMAND ----------

#singles = full_business_95.sort("interval_tm_start", ascending=True)
#list_of_meters = ["4894042"]
#display(singles.filter(col("meter_number").isin(list_of_meters)))
#singles \
#  .coalesce(1) \
#  .write.option("header", True) \
#  .option("sep", ",") \
#  .mode("overwrite") \
#  .csv(f"{path}{name}_individuals")
#display(singles)

# COMMAND ----------

# MAGIC %md
# MAGIC # E) Group up to summary level

# COMMAND ----------

# MAGIC %md
# MAGIC #### Job setup
# MAGIC 1. Get min outage job start per meter
# MAGIC 2. min job start - interval (this rebases the interval for reporting)
# MAGIC 3. Filter out >= 0 on step 2 to focus only on our intervals

# COMMAND ----------

window = Window.partitionBy(col("meter_number"))


job_timeline = full_business_95 \
  .withColumn("min_job_out_tm", min(col("out_dt_tm")).over(window)) \
  .withColumn("interval_from_job_end", unix_timestamp(col("interval_tm_stop")) - unix_timestamp(col("min_job_out_tm"))) \
  .filter(col("interval_from_job_end") >= 0)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Carry job event forward and recenter the intervals for each job
# MAGIC Filter out the negative numbers and null event_index

# COMMAND ----------

meter_window = Window.partitionBy(col("meter_number")).orderBy(col("time_step").asc())

final_with_fill = job_timeline \
  .withColumn("event_index_fill", last('event_number', ignorenulls=True).over(meter_window.rowsBetween(Window.unboundedPreceding, 0))) \
  .withColumn("job_on_dt_tm_fill", last('on_dt_tm', ignorenulls=True).over(meter_window.rowsBetween(Window.unboundedPreceding, 0))) \
  .withColumn("time_from_end_each_job", unix_timestamp(col("interval_tm_stop")) - unix_timestamp(col("job_on_dt_tm_fill"))) \
  .filter(col("event_index_fill").isNotNull()) \
  .filter(col("time_from_end_each_job") >= 0)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Join intervals and group up
# MAGIC 1. Join interval
# MAGIC 2. Take max status per interval
# MAGIC 3. Group up by interval and status
# MAGIC 4. Count status
# MAGIC 5. Add percentages

# COMMAND ----------

cond = (col("time_from_end_each_job") >= col("job_time_step")) & \
      (col("time_from_end_each_job") < col("job_end_time_step"))

#cond =  (col("interval_from_job_end") > col("job_time_step"))

# COMMAND ----------

job_interval_window = Window.partitionBy(col("meter_number"), col("row_reset")).orderBy(col("interval_from_job_end").desc())
job_interval_window = Window.partitionBy(col("meter_number"), col("event_index_fill"), col("job_time_step")).orderBy(col("time_step").desc())

# COMMAND ----------

job_interval_attached = final_with_fill \
  .join(job_interval_data, on=cond, how="left_outer")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get summary statistics across all jobs

# COMMAND ----------

summary_by_job_interval_inservice_job = job_interval_attached \
  .withColumn("job_interval_row", row_number().over(job_interval_window)) \
  .filter(col("job_interval_row") == 1) \
  .groupBy(col("event_index_fill"), col("job_interval"), col("job_time_step"), col("job_end_time_step"), col("final_power_status_95")).agg(count(col("final_power_status_95")).alias("counts"))

# COMMAND ----------

summary_total_by_job_interval_inservice_job = summary_by_job_interval_inservice_job \
  .groupBy(col("event_index_fill"), col("job_interval")) \
  .agg(sum(col("counts")).alias("total_counts"))

# COMMAND ----------

summary_df_job = summary_by_job_interval_inservice_job \
  .join(summary_total_by_job_interval_inservice_job, on=["job_interval", "event_index_fill"] , how="inner") \
  .withColumn("percentage_total", col("counts") / col("total_counts")) \
  .withColumnRenamed("final_power_status_95", "final_power_status") \
  .filter(col("final_power_status").isNotNull())

# COMMAND ----------

event_job_window = Window.partitionBy(col("event_index_fill")).orderBy(col("job_time_step").asc())

num_events = summary_df_job.select("event_index_fill").distinct().count()

filtered_interval_job = summary_df_job \
  .filter((col("percentage_total") >= 0.95) & (col("final_power_status_95") == "Yes")) \
  .withColumn("row_num", row_number().over(event_job_window)) \
  .filter(col("row_num") == 1) \
  .withColumn("mid_point_time_step", ((col("job_time_step") + col("job_end_time_step")) / 2) / 3600) \
  .withColumn("num_events", lit(num_events)) \
  .orderBy(col("mid_point_time_step").asc()) \
  .withColumn("median_percent", monotonically_increasing_id() / col("num_events"))

median_df = filtered_interval_job \
  .filter(col("median_percent") >= 0.5) \
  .orderBy(col("median_percent").asc()) \
  .first()

summary_stat_df = filtered_interval_job \
  .agg(mean(col("mid_point_time_step")).alias("average_time_to_95"),
       min(col("mid_point_time_step")).alias("min_time_to_95"),
       max(col("mid_point_time_step")).alias("max_time_to_95")) \
  .withColumn("median_time_to_95", lit(median_df.mid_point_time_step))

display(summary_stat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the summary of total timeline

# COMMAND ----------

summary_by_job_interval = job_interval_attached \
  .withColumn("job_interval_row", row_number().over(job_interval_window)) \
  .filter(col("job_interval_row") == 1) \
  .groupBy(col("job_interval"), col("job_time_step"), col("final_power_status_95")).agg(count(col("final_power_status_95")).alias("counts"))

# COMMAND ----------

summary_total_by_job_interval = summary_by_job_interval \
  .groupBy(col("job_interval")) \
  .agg(sum(col("counts")).alias("total_counts"))

# COMMAND ----------

summary_df = summary_by_job_interval \
  .join(summary_total_by_job_interval, on=["job_interval"] , how="inner") \
  .withColumn("percentage_total", col("counts") / col("total_counts")) \
  .withColumnRenamed("final_power_status_95", "final_power_status") \
  .filter(col("final_power_status").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC # F) Write to folder location

# COMMAND ----------

display(summary_df)

# COMMAND ----------

summary_df \
 .coalesce(1) \
 .write.option("header", True) \
 .option("sep", ",") \
 .mode("overwrite") \
 .csv(f"{path}{name}")

# COMMAND ----------

summary_stat_df \
 .coalesce(1) \
 .write.option("header", True) \
 .option("sep", ",") \
 .mode("overwrite") \
 .csv(f"{path}{name}-summary-stats")
