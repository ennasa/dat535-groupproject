# Test data

`fangstdata_2023_sample.csv` contains the header and first two records from the
2023 catch and vessel CSV explored in `notebooks/01_explore_source.ipynb`.
It exercises the actual 133-field schema, including Unicode, dotted names,
parentheses, empty values and comma decimals.

Source: [Fiskeridirektoratet](https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata).
Licence: NLOD. Other test cases generate small archives and invalid records in
temporary directories; tests do not download annual source files.
