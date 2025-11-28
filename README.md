### dbt models for AutomateDV Databricks Demonstration

This is an example dbt project, based on the downloaded project [AutomateDV](https://github.com/Datavault-UK/automate-dv), demonstrating
the creation of a Data Vault 2.0 Data Warehouse in Databricks based on the Databricks TPC-H sample dataset.

It has been updated:
* To persist dbt documentation in Unity Catalog
* To utilise native hashes (binary rather than string)
* To implement datacContracts for hub and link materialisations
* To include Constraints definitions of primary keys and foreign keys - so they are reflected in Unity Catalog
* To include Data Tests to verify integrity of primary keys and foreign keys

---

#### AutomateDV Docs
[![Documentation Status](https://readthedocs.org/projects/dbtvault/badge/?version=latest)](https://automate-dv.readthedocs.io/en/latest/?badge=latest)

Click the button above to read the latest AutomateDV docs.

A step-by-step user guide for using this demo is available [here](https://automate-dv.readthedocs.io/en/latest/worked_example/)

---
[dbt](https://www.getdbt.com/) is a registered trademark of dbt Labs).

Check them out below:

#### dbt Docs
- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [Installation](https://dbt.readme.io/docs/installation)
- Join the [chat](http://ac-slackin.herokuapp.com/) on Slack for live questions and support.
---
