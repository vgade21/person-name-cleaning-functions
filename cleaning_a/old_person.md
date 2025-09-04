#### **IMPORTANT NOTE when running person table scripts**

The [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) script is where the main cleansing and transformation (splitting of given_names) of the `person` table is happening.
The [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person) script is where the `person` linking and de-duplication work is happening.

If the `cms_clean` tables (in particular person, business, employee, contact or address) **haven't changed** since the last run **and** the script for [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) **hasn't changed**, the [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) and [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person) can be run independently of each other.
- If they are run together [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) should always be run **before** [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person).

If the `cms_clean` tables (in particular person, business, employee, contact or address) **have changed** since the last run **or** the [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) script **has changed**, then both [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) and [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person) should be run together, with [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) always run **before** [person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/person).
- Running [old_person.py](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&path=/src/systems/cms_clean/old_person) in isolation would prevent the changes (assuming there have been some) to be passed on to the final person table in `cms_staging`, causing downstream issues.

# Transformation

##### DQI:
[32428](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32428)
[32377](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32377)
[32376](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32376)
[32314](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/32314)
[31661](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31661)
[31657](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31657)
[31651](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31651)
[31650](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31650)
[31647](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31647)
[31646](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31646)
[31643](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31643)
[36352](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/36352)
[31653](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31653)
[31584](https://dev.azure.com/emstas/Program%20Unify/_workitems/edit/31584)

### Note: the cleansing and transformation done by the `old_person.py` script is listed in the [person Readme](https://emstas.visualstudio.com/Program%20Unify/_git/unify_2_1_source_to_staging?version=GBdevelop&anchor=important-note-when-running-person-table-scripts&path=/src/systems/cms_clean/person/person.md&_a=preview).
