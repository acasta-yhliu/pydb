# PYDB

Relational Database with Python

#### Building & Running

1. Install requirements

You must install Python >= 3.9 to run this project.

```bash
pip install -r requirements.txt
```

2. Start Up

```bash
python main.py
```

or to run a single file, like `data/create.sql`

```bash
python main.py -f data/create.sql
```

or to start up debug mode

```bash
python main.py -d True
```

or you just want to make them together...

#### Usage

To exit from database, just enter `.exit` and I highly recommend you to do so in order to keep the file integrity. Although SIGINT is caught, there's still possibility to break the whole file if you don't quit normally.

To view grammar, just use `.help`

Then you could use SQL with the database