# scouting-data-ingest
Ingest scouting data from TBA and scouts and transfer it to Tableau and other visualizations

## Set Up

Firstly, clone this project with: 

```
git clone https://github.com/team4099/scouting-data-ingest.git

```

This will create a folder with the Data Ingest App inside. Move into this folder and then make a python venv with:

```
python -m venv ./venv
```

Install needed packages with:

```
pip install -r requirements.txt
```


## Configuration

Create a new folder in the directory called config

Inside this folder you should have 2 files: config.json and a google service account credentials file

### config.json

Your config file should look like this:

```json
{
  "TBA-Key": "[Include your TBA API key here]",
  "Year":"2020",
  "Google-Credentials": "[Include the name of your google service account credentials file here]",
  "Spreadsheet": "[Include the name of the spreadsheet to use here]",
  "Database User": "[User]",
  "Database Password": "[Password]"
}
```


### Google Service Account Credentials File

You will need a Google Service Account to use this. Ask to be added to the project on Google Developer console. Once added, navigate to the credentials page and click on the service account. Move to the details tab and add a new json key. Download the file and move into the config folder. In config.json, replace the Google Credentials value with the name of your file.
