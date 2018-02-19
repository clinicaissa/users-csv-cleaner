#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Name: CSV of Users cleaner script
# Description: The script allows to clean an instance of the current
# database of insurance policies owners and beneficiaries into a clean CSV
# format, cleaning phone numbers and invalid records
#
# Usage: python transform.py <users_file>
import sys
import os
import re
import csv
import pandas as pd
import numpy as np
import logging
import datetime

# Constants
CURRENT_DATE_NP = np.datetime64(datetime.datetime.now())
"""
Current date and time to detect past and future times
"""

# # Source parameters
SRC_COL_DELIMITER = ","
SRC_ROW_DELIMITER = "\n"
SRC_QUOTE_CHAR = "\""
SRC_COLS_DROP = [
    'N.Orden',       # Not relevant
    'Ind.Estado',    # Not relevant
    'Ind.Baja',      # Rarely used
    'FECHANACIMI',   # No longer used
    'ID_PLZ1',        # Not relevant
    'FECHAINSCRI',   # No longer used
    'Grupos',        # Not relevant
    'CENTRO',        # Not relevant
    'CLAVECARTERA',  # Not relevant
    'CLAVEPOLIZA'    # Not relevant
]
SRC_COLS_RENAME = {
    "N.Poliza": "NUM_POLIZA",
    "Nombre": "Given Name",
    "Apellidos": "Family Name",
    "Parentesco": "RELACION_POLIZA",
    "Siglas Estado": "ESTADO_CIVIL",
    "Sexo": "Gender",
    "Fecha Nacimiento": "Birthday",
    "Fecha Inscripcion": "FECHA_ALTA",
    "E.Mail": "EMAIL",
    "NIF": "NIF"
}
SRC_COLS_DATETIME = ['Fecha Nacimiento', 'Fecha Inscripcion']


# # # Parser
def SRC_DATE_PARSER(date):
    if pd.isnull(date):
        return date
    # Final date
    date = date.replace("/", "")
    # Correct date
    if len(date) != 6 or len(date) != 8:
        # Replace full years
        re.sub(r"19(\d{2})", r"\1", date)
    # Add slashes
    date = date[:2] + "/" + date[2:4] + "/" + date[4:]
    # Two-digit year
    if len(date) == 8 and re.match("^\d{2}/\d{2}/\d{2}$", date):
        parsed = pd.datetime.strptime(date, "%d/%m/%y")
    # Four-digit year
    if len(date) == 10:
        parsed = pd.datetime.strptime(date, "%d/%m/%Y")
    # Final result
    return parsed


# # # Transformations
def UNIFORM_NAMES(name):
    # Multiple spaces are not allowed
    name = re.sub(' +', ' ', name).strip()
    # Uniform quotation
    name = name.replace('`', "'")
    # No space after quotation
    name = name.replace("' ", "'")
    # Title
    name = name.title()
    # Replace prepositions
    name = name.replace(" Del", " del")
    name = name.replace(" De", " de")
    return name


def SRC_TRF_NOMBRE(nombre):
    return UNIFORM_NAMES(nombre) if isinstance(nombre, str) else nombre


EMAIL_REGEXP = re.compile(
    r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
"""
    regexp: email 99.99p success regexp
"""


def SRC_TRF_EMAIL(email):
    if isinstance(email, str):
        email = email.strip().lower()
        if EMAIL_REGEXP.match(email):
            return email
    return ""


def SRC_TRF_SEXO(sexo):
    return "M" if sexo == "H" else "F" if sexo == "M" else ""


def SRC_TRF_NIF(nif):
    # Nulls
    if pd.isnull(nif):
        return nif
    # NIF correction
    return nif.strip().replace("-", "").replace(" ", "").upper()


SRC_TRANSFORMATIONS = {
    "Nombre": SRC_TRF_NOMBRE,
    "Apellidos": SRC_TRF_NOMBRE,
    "E.Mail": SRC_TRF_EMAIL,
    "Sexo": SRC_TRF_SEXO,
    "NIF": SRC_TRF_NIF
}

SRC_COLS_TYPES = {
    "Sexo": "category"
}

# # Destination parameters
DST_COL_DELIMITER = ","
DST_COLS_QUOTING = csv.QUOTE_NONNUMERIC
DST_ROW_DELIMITER = "\n"
DST_QUOTE_CHAR = "\""
DST_COLS_OUTPUT = \
    ["NUM_POLIZA", "Given Name", "Family Name", "RELACION_POLIZA",
     "ESTADO_CIVIL", "Gender", "Birthday", "FECHA_ALTA", "E-mail 1 - Type",
     "E-mail 1 - Value", "Phone 1 - Type", "Phone 1 - Value", "Phone 2 - Type",
     "Phone 2 - Value", "NIF", "Source"]


def DST_COL_TELEFONO_SPLITTER(telfs):
    if isinstance(telfs, str):
        # replace multiple spaces
        telfs = re.sub("[^0-9 ]", "", telfs)
        telfs = re.sub("\ +", " ", telfs).strip()
        return telfs.split()
    else:
        return None


def DST_COL_TELEFONO(telf):
    print("before split", telf)
    tels = DST_COL_TELEFONO_SPLITTER(telf)
    print("after split", tels)
    if tels is not None:
        for tel in tels:
            # 7-digit number, missing +93
            match = re.match("^\d{7}$", tel)
            if match is not None:
                return "93" + match.group(0)
            # 9-digit number, non-starting per 6/7
            match = re.match("^[^67]\d{8}$", tel)
            if match:
                return match.group(0)
    return ""


def DST_COL_TMOVIL(telf):
    print("before split", telf)
    tels = DST_COL_TELEFONO_SPLITTER(telf)
    print("after split", tels)
    if tels is not None:
        for tel in tels:
            # 9-digit number, starting per 6/7
            match = re.match("^[67]\d{8}$", tel)
            if match is not None:
                return match.group(0)
    return ""


def DST_COL_TYPE_HOME(self):
    return "Home"


def DST_COL_TYPE_MOBILE(self):
    return "Mobile"


def DST_COL_TYPE_SOURCE(self):
    return "ISSADB"


DST_COLS_GENERATED = {
    "Phone 1 - Value": ("Telefono", DST_COL_TMOVIL),
    "Phone 2 - Value": ("Telefono", DST_COL_TELEFONO),
    "E-mail 1 - Value": ("E.Mail", SRC_TRF_EMAIL),
    "E-mail 1 - Type": ("E.Mail", DST_COL_TYPE_HOME),
    "Phone 1 - Type": ("E.Mail", DST_COL_TYPE_MOBILE),
    "Phone 2 - Type": ("E.Mail", DST_COL_TYPE_HOME),
    "Source": ("E.Mail", DST_COL_TYPE_SOURCE),
}

DST_COLS_TYPES = {
    "SEXO": "category"
}

DST_COLS_MANDATORY = ["Given Name", "Family Name", "Phone 1 - Value"]

# Config
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(name)s][%(levelname)s] %(message)s"
)

# Read arguments
args = sys.argv[1:]
if len(args) < 1:
    logging.error("Specify the file to transform")
    logging.error(
        "Usage: python transform.py <users_file>")
    sys.exit(1)
users_file = args[0]
logging.info("Using file %s as users file" % users_file)

# Read files
# # Input file
logging.info("Reading file %s" % users_file)
users = pd.read_table(
    users_file,
    header=0,
    delimiter=SRC_COL_DELIMITER,
    quotechar=SRC_QUOTE_CHAR,
    parse_dates=SRC_COLS_DATETIME,
    date_parser=SRC_DATE_PARSER
)
logging.info(" - Set first row as header")
logging.info(" - Found %d records", len(users))
logging.info(" - Headers are %s", users.columns)

# Transform
logging.info("=== Applying transformations ===")
# # 1. Clean invalid columns
logging.info("1. CLEAN invalid columns")
logging.info("   Remove no-longer-used columns: %s", SRC_COLS_DROP)
users = users.drop(SRC_COLS_DROP, 1)
# # 2. Rename columns
logging.info("2. RENAME valid columns")
users.rename(inplace=True, columns=SRC_COLS_RENAME)
logging.info("   Updated headers are %s", SRC_COLS_RENAME)
# # 3. Internal formatting
logging.info("3. READ COLUMNS format")
logging.info("   Columns %s are datetimes",
             list(map(SRC_COLS_RENAME.get, SRC_COLS_DATETIME)))
for col, dtype in SRC_COLS_TYPES.items():
    logging.info("   Column %s is type <%s>", col, dtype)
    users[SRC_COLS_RENAME.get(col, col)] = \
        users[SRC_COLS_RENAME.get(col, col)].astype(dtype)
# # 4. Extra columns
logging.info("4. ADDING new columns")
for col, creator in DST_COLS_GENERATED.items():
    src_col, creator = creator
    logging.info("   Creating column %s from column %s", col, src_col)
    # Apply creation transformation
    src_col = SRC_COLS_RENAME.get(src_col, src_col)
    new_col = users[src_col].aggregate(creator)
    # Set type
    if col in DST_COLS_TYPES:
        new_col = new_col.astype(DST_COLS_TYPES[col])
    # New column details
    if isinstance(new_col.dtype, pd.core.dtypes.dtypes.CategoricalDtype):
        logging.info("   -> New categorical distribution:")
        lines = str(new_col.value_counts()).split("\n")
        for line in lines:
            logging.info("      %s", line)
    else:
        logging.info("    -> New column:")
        lines = str(new_col.describe()).split("\n")
        for line in lines:
            logging.info("      %s", line)
    # Append to dataframe
    users[col] = new_col
# # 5. Format transformations
logging.info("5. APPLYING COLUMN transformations")
for col, transform in SRC_TRANSFORMATIONS.items():
    # Renaming
    col = SRC_COLS_RENAME.get(col, col)  # Not renamed maybe
    logging.info("   Transforming column %s", col)
    # Apply transformation
    old_col = users[col]
    new_col = old_col.apply(transform)
    # Set type
    if col in SRC_COLS_TYPES:
        new_col = new_col.astype(SRC_COLS_TYPES[col])
    # Check distribution if categorical
    if isinstance(old_col.dtype, pd.core.dtypes.dtypes.CategoricalDtype):
        logging.info("   -> Old distribution:")
        lines = str(old_col.value_counts()).split("\n")
        for line in lines:
            logging.info("      %s", line)
    else:
        logging.info("    -> Old column:")
        lines = str(old_col.describe()).split("\n")
        for line in lines:
            logging.info("      %s", line)
    if isinstance(new_col.dtype, pd.core.dtypes.dtypes.CategoricalDtype):
        logging.info("   -> New distribution:")
        lines = str(new_col.value_counts()).split("\n")
        for line in lines:
            logging.info("      %s", line)
    else:
        logging.info("    -> New column:")
        lines = str(new_col.describe()).split("\n")
        for line in lines:
            logging.info("      %s", line)
    # Save transformation
    users[col] = new_col

# # 7. Clean invalid rows
logging.info("7. CLEAN invalid rows")
logging.info("   Rows without %s", DST_COLS_MANDATORY)
# # # Null cleans
for col in DST_COLS_MANDATORY:
    users = users[pd.notnull(users[col])]
logging.info("   -> Without nulls: %d records", len(users))
# # # Invalid strings
for col in DST_COLS_MANDATORY:
    if users[col].dtype == np.object:
        users = users[users[col].map(len) > 0]
logging.info("   -> Without empty strings: %d records", len(users))
# # 8. Write results
logging.info("8. WRITING result")
logging.info("   ColDelimiter=%s | RowDelimiter=%s",
             DST_COL_DELIMITER, DST_ROW_DELIMITER)
logging.info("   Export columns: %s", DST_COLS_OUTPUT)
DST_FILE_NAME = ".".join(os.path.basename(users_file).split(".")[:-1])
DST_FILE_EXT = os.path.basename(users_file).split(".")[-1]
DST_FILE = DST_FILE_NAME + "_converted." + DST_FILE_EXT
logging.info("   Export name: %s", DST_FILE)
delimiter = DST_COL_DELIMITER
users.to_csv(DST_FILE,
             index=False,
             columns=DST_COLS_OUTPUT,
             sep=delimiter,
             line_terminator=DST_ROW_DELIMITER,
             quoting=DST_COLS_QUOTING,
             date_format="%Y-%m-%d")
logging.info("CONVERSION finished")
