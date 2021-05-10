# Adding custom sql

This directory contains two examples of custom modules. The directories follow the same naming convention as the standard module, whereby each directory is assigned a number corresponding to the level of aggregation that the SQL is concerned with. In addition to these examples, below is a guide to creating custom modules.

## Guidelines & Best Practice

The v1 Model's modular structure allows for custom SQL modules to leverage the model's incrementalisation logic, and operate as 'plugins' to compliment the standard model. This can be achieved by using the `_staged` tables as an input, and producing custom tables which may join too the standard model's main production tables (for example, to aggregate custom contexts to a screen_view level), or provide a separate level of aggregation (for example a custom user interaction).

The standard modules carry out the heavy lifting in establishing an incremental structure and providing the core logic for the most common mobile aggregation use cases. It also allows custom modules to be plugged in without impeding the maintainence of standard modules.

The following best practices should be followed to ensure that updates and bugfixes to the model can be rolled out with minimal complication:

- Custom modules should not modify the `_staged` tables
- Custom modules should not modify the standard model's production tables (eg `mobile_screen_views`, `mobile_sessions` and `mobile_users`) - adding extra fields to the production tables can be achieved by producing a separate table which joins to the production table.
- Custom modules should not modify any manifest tables.
- Customisations should not modify the SQL in the standard model - they should only comprise of a new set of SQL statements, which produce a separate table.
- The logic for custom SQL should be idempotent, and restart-safe - in other words, it should be written in such a way that a failure mid-way, or a re-run of the model will not change the deterministic output.

In short, the standard modules can be treated as the source code for a distinct piece of software, and custom modules can be treated as self-maintained, additive plugins - in much the same way as a Java package may permit one to leverage public classes in their own API, and provide an entry point for custom programs to run, but will not permit one to modify the original API.

The `_staged` and `_this_run` tables are considered part of the 'public' class of tables in this model structure, and so we can give assurances that non-breaking releases (ie. any v1.X release) won't alter them. The other tables may be used in custom SQL, but their logic and structure may change from release to release, or they may be removed. If one does use a scratch table in custom logic, any breaking changes can be mitigated by either amending the custom logic to suit, or copying the relevant steps from an old version of the model into the custom module. (However this will rarely be necessary).

## Interacting with the model structure

Each standard module produces a `_staged` table which serves as the input to the next module. This should also serve as the input to custom modules. For example, the `01-base` module produces the `scratch.mobile_events_staged` table - this is a subset of the pipeline's `events`, with the addition of various optional contexts, containing only data from sessions which contain new data in this run of the model. To aggregate atomic data, one can read from the `scratch.mobile_events_staged` table.

Each standard module also contains a `99-{module}-complete` playbook, which completes the incremental logic for the previous step by truncating the input\*. This should run after both the custom and standard module have run. So, for our use case of aggregating atomic data, we would:

  1. run the `01-base` module, to produce the `mobile_events_staged` table
  2. run both the standard module and custom modules
  3. Run the `99-sessions-complete` playbook (which truncates `events_staged`)

For less common requirements, one can opt not to run a given standard module, or to wait until the end of the model before running all `99-{module}-complete` steps. For example, if the custom logic requires, the following order of operations is acceptable:

  1. run `01-base`
  2. run `02-screen-views`
  3. run `02a-custom-sv-entity-aggregations`
  4. run `03-sessions`
  5. run `03a-custom-sessions-aggregations`
  6. run `04a-custom-users-aggregations` (note no standard users module)
  7. run `99-screen-views-complete`
  8. run `99-sessions-complete`
  9. run `99-users-complete`

\* The exception to this is the screen views and optional modules. Their `99-{module}-complete` playbook does not truncate their input, `mobile_events_staged`. This is because the session module requires `mobile_events_staged` as an input and therefore the truncation occurs within the sessions module.

## Producing custom tables

### Design

As mentioned above, custom modules should be written as additive-only, and should produce separate custom tables, as distinct from the tables produced by the standard module. If the requirement of the customisation is to add fields to compliment the production tables, this can be done in one of two ways: a) create a table which joins to the standard table on its joinkey, or b) create a custom table which duplicates the interesting fields from the standard model, to a new table which also contains the custom data.

For example, at screen views level, one can either:

a) Create a new table `mobile_screen_views_additions`, which is one-row-per screen_view_id, and contains the relevant data.

or b) Create a new table `mobile_screen_views_custom`, which contains the relevant customisations joined to the relevant fields from `mobile_screen_views`.

One should not amend `mobile_screen_views` directly.

### Implementation

The easiest way to integrate with the model's incremental structure is to implement a three-step process for custom tables:

1. Write the relevant aggregation logic using drop and recompute logic, to produce a `_staged` table (only interacting with the input data)
2. Use the provided `derived.commit_table()` procedure to commit to the production table.

The standard model's incrementalisation logic ensures that all relevant data for a given row will be in the input, and only data for sessions which contain new data will be included. `commit_table` will do the job of creating the production table, or adding new columns as required. Note that `commit_table` requires a time key for partitions.

Users who wish to specify exact constraints or additional features of the production table (eg. cluster keys) may create it first in SQL using a `CREATE TABLE statement`.

An example of this can be seen in the `04-session-goals` directory, which makes use of the commit table procedure to update the production table.

The arguments to the commit table procedure are as follows:

```SQL     

CALL derived.commit_table('scratch',              -- source schema
                          'session_goals_staged', -- source table
                          'derived',              -- target schema
                          'session_goals',        -- target table
                          'session_id',           -- join key
                          'start_tstamp',         -- partition key
                           TRUE);                 -- automigrate
```

If `automigrate` is TRUE, tables which don't exist will be created, and new columns will be added to the target table. If FALSE, the query will fail without committing unless the target table exists and columns match exactly.

## Advanced usage - variable scheduling and non-standard requirements

As mentioned above, the model's structure allows for some more complex use cases which may require a nuanced approach. Where this is required, it is advisable to begin by setting up the model in a standard way first, iterate upon it insofar as possible, and move to more complex requirements once the nuance is well understood.

### Variable scheduling of modules with customisations

The `_staging` tables update incrementally, and so it is possible to vary the schedule of different modules of the model without impact on the incremental structure. For example, one can run `01-base` and `02-screen-views` every hour, `04-sessions` once a day and `05-users` once a week.

This is possible because every time `02-screen-views` runs, it incrementally updates the `mobile_screen_views_staged` to include all new data since the last run of the `04-sessions` module.

Sometimes, one might require custom modules to run on a more frequent schedule than their standard counterparts. For example, one might wish to run a custom module to aggregate transaction events (from atomic data) every 30 mins, but only run the `02-screen-views` module once a day.

This is possible using the above described structure - as long as the custom module is written to `DELETE` and `INSERT`, then it will remain accurate with every run. However, it is inefficient - every run of the custom module will process _all_ data since the last run of the `02-screen-views` module, including the data that's already been processed in the custom module.

To allow for this kind of requirement, each module _also_ produces a `_this_run` table, which contains only the data for the _current_ run of the module. If one requires a custom module to run more frequently than a standard one - and aims for the most efficient means of doing so, one may use the `_this_run` table as an input.

Do note that this requires that the custom module runs every time the previous module runs. So if using `_this_run` as an input, it is acceptable to run two jobs as follows:

1. `01-base`, `02a-custom`
2. `01-base`, `02a-custom`, `02-screen-views`

But not as follows:

1. `01-base`, `02a-custom`
2. `01-base`, `02-screen-views`

Since in the latter, running job 2 processes data which should be included in `02a-custom`, and that data is never persisted to the input of `02a-custom`.
