### EZParseSQL

Extract SQL Target and Source

Make Report of every Script Taget & Source Table

Help Usage

`python3 main.py -h  `


```batch
# Single File
python3 main.py -S FILE_PATH

# Multiple files in diectory and recursive
python3 main.py -P FILE_FOLDER

# Multiple files in specific pattern
python3 main.py -P FILE_FOLDER --pattern="*.sql"

# Multiple files exclude specific patter
python3 main.py -P FILE_FOLDER --exclude=".*OLD.*"


```