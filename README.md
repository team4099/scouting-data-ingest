# scouting-data-ingest
Ingest scouting data from TBA and scouts and transfer it to Tableau and other visualizations


## Configuration

Create a new folder in the directory called config

Inside this folder you should have 3 files: config.json, SQLconfig.json, and a google service account credentials file

### config.json

Your config file should look like this:

```json
{
  "TBA-Key": "[Include your TBA API key here]",
  "Year":"2020",
  "Google-Credentials": "[Include the name of your google service account credentials file here]",
  "Spreadsheet": "[Include the name of the spreadsheet to use here]"
}
```

### SQLconfig.json

This file contains the configuration for the MySQL Database. Attributes is where the column configuration will be saved.

```json
{
    "TeamDataConfig": {
        "Year": "2020",
        "Attributes": {
        }
    },
    "MatchDataConfig": {
        "Year": "2020",
        "Attributes": {
        }
    }
}

```

### Google Service Account Credentials File

You will need a Google Service Account to use this. Make one in the Google Developer Console and give it Drive and Sheets API Access. 
Create service account credentials in the Credentials section. Download the file and keep it safe. Place the file in the config directory. Change the corresponding name
in config.json. Make sure to share your spreadsheet with the service account's email.
