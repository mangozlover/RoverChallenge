

# GatherAndStore

This is a python package created for the RoverChallenge which pulls data from a Socrata endpoint and writes to a MySQL database with options to use other data stores using the correct client libraries.  It has been written to be extensible simply by adding new IMPORT_TOPICs to the settings.py file.  An IMPORT_TOPIC is the term I use to specify a data set that we would want to gather.  

Answers to the Rover Challenge questions can be found in RoverChallenge_Answers.docx in the root of the project.


## Project structure

```
  |RoverChallenge
  ├── sql
  │   ├── schema  ..............................................scripts to create rover db & schema
  │   │   ├── 0_create_db_rover.sql
  │   │   ├── 1_create_table_import_log.sql
  │   │   └── 2_create_table_pet_license.sql  
  │   ├── users
  │   │   └── create_user_importuser.sql .......................create user/GRANT
  │   └── procs
  │       └── 0_create_proc_GetLicenseCountBySpeciesYear.sql ...used for data visualization
  ├── dataflow
  │   ├── gather_and_store.py...................................main code for application
  │   ├── csv_serializer.py.....................................helper to sanitize data
  │   ├── mysql_wrapper.py......................................helper to wrap MySQL calls
  │   └── settings.py...........................................application config file  
  ├── requirements.txt..........................................used by pip to install project
  └── RoverChallenge_Answers.docx...............................Rover Challenge questions answered
  ```

## Version 1.0 features

- Ability to use various target data stores
- Ability to add new import datasets by adding settings in settings.py
- Imports can be incremental or complete.  Each dataset has a filter (in settings.py) where clause so that updates to data can be incremental.  In the case of "pet_license", this filter is *where license_issue_date > MAX_stored_license_issue_date*.
- Ability to initialize (or re-initialize) the data by using the passed argument **--initialize**.  This will truncate the target table and start importing the entire dataset.
- Paging for large amounts is controlled with a setting. If row count of pulled data is less than the settings limit, then data is pulled all at once. Otherwise the data is paged.
- Size of data chunks is a parameterized setting.
- Each import operation is logged to a database using a separate client. Import logging message is written at the beginning before the first insert occurs.  A logging row is written upon successful completion.  And a logging row is written for each chunk of rows upon success.


## Lessons learned and NEXT version

Oh man!  I knew it would be slow, but I didn't realize how slow.  It takes almost 2 hours to pull and store all 51,754 rows with a limit of 1000 per chunk.  I wanted to get this out the door in a timely manner to be competitive with other candidates so I cut some things short.  In the second version I would look into a python library to take advantage of bulk loading in MySQL (*LOAD DATA INFILE*).  This would be achievable using *pandas* and *pymysql*.


## Usage

### Arguments:  
| Arg | Required | Description                            |
| :--- | :---:| :--- |
| -t  or --import_topic | Yes | Specify data import topic, ex. pet_license |
| --initialize | No | To import ALL data from scratch or truncate target and start over, add --initialize |

```
usage: gather_and_store.py [-h] -t IMPORT_TOPIC [--initialize]

optional arguments:
  -h, --help            show this help message and exit
  -t IMPORT_TOPIC, --import_topic IMPORT_TOPIC
                        Specify data import topic, ex. pet_license
  --initialize          To import ALL data from scratch or truncate target and
                        start over, add --initialize
```

### Example calls:
Incremental data pull and application logging will print to the console.
```
./gather_and_store.py -t pet_license
```

Full data pull (truncate target and pull all source data) and application logging will print to the console.
```
./gather_and_store.py -t pet_license --initialize
```


## Getting Started

The instructions in Prerequisites and Installing will get you a copy of the project up and running on your local machine for development and testing purposes.


### Prerequisites

* Python 2.7.  
In order to run as intended, please have an installation of Python2.7 available.  Download of Python 2.7 installers and source code can be found [here](https://www.python.org/download/releases/2.7/).

* pip
Using pip with the requirements.txt insures that all required packages and versions are present.

* MySQL server with a database (creation script included)
Please run the scripts under folder **SQL/schema/** in the order in which they alphanumerically sort.  Then run the single script under **SQL/users** to create the user employed in this system as well as the necessary permissions.

* OS consideration (Ubuntu and Debian - other operating systems not tested)
My OS was a newer version of Ubuntu and I was required to install these packages to get my requirements.txt to install cleanly.  I was able to cleanly install on Debian.
```
sudo apt-get install default-libmysqlclient-dev
sudo apt-get install libperl-dev
sudo apt-get install libgtk2.0-dev
sudo apt-get install libgirepository1.0-dev
```


### Installing

Once those OS packages are installed, download the project repo onto your local machine and run pip to install the requirements.

```
cd /path/to/RoverChallenge/
pip install -r requirements.txt
```

Modify the MySQL connection info in settings.py to match your database connection information.

The project is ready to run.  See the "Usage" section to see how to fire it off.


## Running the tests

There are some basic tests in cvs_serializer.py which gives an idea of how this could be tested.  In order to release this to Rover in a timely manner, I am choosing to write test before this gets put into a CICD pipeline in preparation for release to Production.



## Built With

* [Python2.7](https://docs.python.org/2/) - The language used to build this package


## Authors

* **Jeff Abbott**
