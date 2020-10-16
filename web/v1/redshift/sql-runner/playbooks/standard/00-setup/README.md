# Setup step playbooks

These playbooks exist to accommodate scheduling jobs which don't conform to the usual flow of the standard model - they create and destroy the necessary tables to log metadata, with an ID which persists across modules.

If running the standard model alone, or the standard model alongside custom steps, it is not necessary to run these steps. Instead, we can configure the `:ends_run:` variable to `true` in the `complete` playbook for the last module run in the standard model.

In a scenario where we don't run the standard module, or we run portions of it on differing schedules, we can run the `00-setup-metadata.ymp.tmpl` playbook as the first step - to set up metadata and create the temporary run ID, and the `99-complete-metadata.yml.tmpl` playbook as the last step - to destroy the temporary run ID.

If we would like to destroy the metadata tables for a full rebuild of the model, we may run the `XX-destroy-metadata.yml.tmpl` playbook to do so. It is advisable to rename the metadata table instead, however, in case there is some unforeseen need to see that data.
