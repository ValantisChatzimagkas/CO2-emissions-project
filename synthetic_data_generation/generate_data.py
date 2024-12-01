import datetime
import os
import time
from enum import Enum
from typing import List
import psycopg2
from loguru import logger
from sqlalchemy import Table, Column, Integer, Text, DECIMAL, ForeignKey, String, select
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from faker import Faker
from random import randint, uniform, choice
from itertools import product

DATABASE_NAME = "emission_db"

Faker.seed(randint(-10 ** 6, 10 ** 6))
fake = Faker()

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))


class BaseOptionsClass(str, Enum):

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class TableOptions(BaseOptionsClass):
    organizations = "organizations"
    emission_sources = "emission_sources"
    emission_factors = "emission_factors"
    emissions_logs = "emissions_logs"


class ColumnOptions(BaseOptionsClass):
    _id = "id"
    scope = "scope"
    source_name = "source_name"
    description = "description"
    source_id = "source_id"
    factor_value = "factor_value"
    unit = "unit"
    calculation_method = "calculation_method"
    name = "name"
    created_at = "created_at"
    organization_id = "organization_id"
    date = "date"
    quantity = "quantity"
    total_emissions = "total_emissions"


EMISSION_SCOPE_OPTIONS = ["1", "2", "3"]
EMISSION_SOURCES_OPTIONS = ['Electricity', 'Natural Gas', 'Diesel Fuel', 'Gasoline', 'Coal', 'Diesel Trucks',
                            'Passenger Vehicles', 'Air Travel', 'Shipping (Maritime)', 'Rail', 'Cement Production',
                            'Steel Manufacturing', 'Chemical Manufacturing', 'Paper Mills', 'Refineries',
                            'Livestock (Enteric Fermentation)', 'Rice Paddy Cultivation', 'Fertilizer Application',
                            'Forest Conversion', 'Soil Management', 'Landfills', 'Waste Incineration',
                            'Wastewater Treatment', 'Residential Heating', 'Commercial HVAC Systems',
                            'Building Construction', 'Refrigerants (HFCs)', 'CO2 Capture', 'Waste Oil Combustion',
                            'Construction Equipment', 'Agricultural Machinery']


class DataGenerator:
    """
    Generates synthetic data for our project
    """

    def __init__(self, db_name: str):
        """
        Initialize the database connection and metadata.
        :param db_name: SQLite database file name.
        """
        self.db_name = db_name
        self.engine = create_engine(os.environ['DATABASE_CONN'])
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def create_organization(self, how_many: int, table_name: TableOptions = TableOptions.organizations):
        """
        Generates data for organization entities

        :param how_many: specify how many organizations you want to create
        :param table_name: name of table
        :return:
        """

        columns: List = [Column('id', Integer, primary_key=True, nullable=False),
                         Column('name', Text, nullable=False),
                         Column('description', Text, nullable=True),
                         Column('created_at', Text, nullable=True),
                         ]
        table = Table(table_name, self.metadata, *columns)

        data = [{
            "name": fake.company(), "description": fake.bs(), "created_at": datetime.datetime.now().isoformat()
        } for _ in range(how_many)]

        try:
            with self.engine.connect() as connection:
                connection.execute(table.insert(), data)
                connection.commit()
            logger.info(f"Data inserted successfully into table '{table_name}'.")
        except SQLAlchemyError as e:
            logger.info(f"Error inserting data into table '{table_name}': {e}")

    def create_emission_sources(self, table_name: TableOptions = TableOptions.emission_sources):
        """
        Generate all possible combinations of scope and source names and insert them into the database.
        Ensures no duplicates are inserted into the table.
        :param table_name: Name of the table to insert data into.
        """
        # Define table structure
        columns: List = [
            Column('id', Integer, primary_key=True, nullable=False),
            Column('scope', Text, nullable=False),
            Column('source_name', Text, nullable=False),
            Column('description', Text, nullable=True),
        ]
        table = Table(table_name, self.metadata, *columns)

        # Generate all combinations of scope and source_name
        data = [
            {
                "scope": scope,
                "source_name": source_name,
                "description": fake.bs(),
            }
            for scope, source_name in product(EMISSION_SCOPE_OPTIONS, EMISSION_SOURCES_OPTIONS)
        ]

        try:
            with self.engine.connect() as connection:
                # Insert data while ignoring duplicates
                for record in data:
                    insert_query = insert(table).values(**record)
                    # Add ON CONFLICT clause to handle duplicates
                    on_conflict_query = insert_query.on_conflict_do_nothing()
                    connection.execute(on_conflict_query)
                connection.commit()
            logger.info(f"Data inserted successfully into table '{table_name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting data into table '{table_name}': {e}")

    def select_column_from_table(self, table_name: TableOptions, column: Column):
        try:
            table = Table(table_name, self.metadata, column)
            with self.engine.connect() as connection:
                result = connection.execute(select(column).select_from(table)).scalars().all()
                connection.commit()
                logger.info(f"Select query on table '{table_name}' and column: {Column} was successful.")
                return result

        except SQLAlchemyError as e:
            logger.info(f"Error inserting data into table '{table_name}': {e}")

    def create_emission_factors(self, table_name: TableOptions = TableOptions.emission_factors):
        """
        Generate emission factors for all existing emission sources and insert them into the database.
        Ensures no duplicates are inserted into the table.
        :param table_name: Name of the table to insert data into.
        """
        # Define table structure
        columns: List = [
            Column('id', Integer, primary_key=True, nullable=False),
            Column('source_id', Integer, ForeignKey('emission_sources.id'), nullable=False),
            Column('factor_value', DECIMAL, nullable=False),
            Column('unit', String(50), nullable=False),
            Column('calculation_method', Text, nullable=True),
        ]
        table = Table(table_name, self.metadata, *columns)

        # Units and calculation methods
        UNITS = ["kgCO2e/unit", "gCO2e/unit", "tCO2e/unit"]
        CALCULATION_METHODS = [
            "Method A: Direct Measurement",
            "Method B: Emission Factor Multiplication",
            "Method C: Hybrid Approach",
        ]

        try:
            # Retrieve all emission sources
            with self.engine.connect() as connection:
                # Properly define the table object to access its columns
                emission_sources_table = Table(
                    TableOptions.emission_sources,
                    self.metadata,
                    autoload_with=self.engine
                )

                # Query for all `id` values from the `emission_sources` table
                emission_sources_query = select(emission_sources_table.c.id)  # Use the `.c` attribute to access columns
                emission_sources = connection.execute(emission_sources_query).fetchall()

            # Generate data for emission factors
            data = [
                {
                    "source_id": source[0],
                    "factor_value": round(uniform(0.1, 10.0), 3),  # Random factor value between 0.1 and 10.0
                    "unit": choice(UNITS),
                    "calculation_method": choice(CALCULATION_METHODS),
                }
                for source in emission_sources
            ]

            # Insert data while ignoring duplicates
            with self.engine.connect() as connection:
                for record in data:
                    insert_query = insert(table).values(**record)  # Use PostgreSQL-specific Insert
                    on_conflict_query = insert_query.on_conflict_do_nothing()  # Avoid duplicates
                    connection.execute(on_conflict_query)
                connection.commit()

            logger.info(f"Emission factors inserted successfully into table '{table_name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting emission factors into table '{table_name}': {e}")

    def create_emission_logs(self, how_many: int, table_name: TableOptions = TableOptions.emissions_logs):
        """
        Generate a specified number of emission logs and associate them with random organizations.
        :param how_many: Number of emission logs to create.
        :param table_name: Name of the table to insert into.
        :return: None
        """
        try:
            # Retrieve all emission sources and organizations
            with self.engine.connect() as connection:
                # Define the tables
                emission_sources_table = Table(TableOptions.emission_sources, self.metadata, autoload_with=self.engine)
                organizations_table = Table(TableOptions.organizations, self.metadata, autoload_with=self.engine)

                # Query for emission sources and organizations
                emission_sources_query = select(emission_sources_table.c.id)
                organizations_query = select(organizations_table.c.id)

                emission_sources = connection.execute(emission_sources_query).scalars().all()
                organizations = connection.execute(organizations_query).scalars().all()

            # Ensure there are organizations to select from
            if not organizations:
                raise ValueError("No organizations found in the database.")
            if not emission_sources:
                raise ValueError("No emission sources found in the database.")

            # Generate data for emission logs
            data = []
            for _ in range(how_many):
                source = choice(emission_sources)  # Randomly choose an emission source
                organization = choice(organizations)  # Randomly choose an organization

                log_entry = {
                    "source_id": source,  # Use the first element of tuple (ID from emission_sources)
                    "organization_id": organization,  # Randomly choose an organization
                    "date": fake.date_this_decade(),  # Generate a random date within this decade
                    "quantity": round(randint(1, 1000), 2),  # Random quantity between 1 and 1000
                    "total_emissions": round(randint(1, 1000) * 0.5, 2)  # Example emissions calculation
                }
                data.append(log_entry)

            # Insert data into emissions_logs table
            with self.engine.connect() as connection:
                # Define the emissions_logs table
                emissions_logs_table = Table(
                    TableOptions.emissions_logs,
                    self.metadata,
                    autoload_with=self.engine
                )

                # Insert each record into the table
                for record in data:
                    try:
                        insert_query = insert(emissions_logs_table).values(**record)
                        connection.execute(insert_query)
                    except SQLAlchemyError as e:
                        # Log the record that caused the error and the exception message
                        logger.error(f"Error inserting record into {table_name.name}: {record}")
                        logger.exception(e)

                connection.commit()

            logger.info(f"{how_many} emission logs inserted successfully into table '{table_name.name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting emission logs into table '{table_name.name}': {e}")
            logger.exception(e)  # Log the full exception
        except ValueError as e:
            logger.error(f"Error: {e}")


def wait_for_db():
    """Wait for the PostgreSQL database to be ready."""
    retry_counter = 0
    delay = 5  # seconds

    while retry_counter < 3:
        try:
            # Try to connect to the PostgreSQL server
            connection = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME')
            )
            connection.close()  # Close the connection immediately if successful
            logger.info("Database is ready!")
            break
        except psycopg2.OperationalError:
            # If connection fails, wait 5 seconds and retry
            logger.info("Waiting for the database to be ready...")
            time.sleep(delay)
            retry_counter += 1

    logger.error(f'Failed to connect to database')


def generate_data():
    """
    Executes step by step the synthetic data generation for our project
    :return:
    """
    data_generator = DataGenerator(db_name="emission_db")

    logger.info("Step 1 - Generate Organizations")
    data_generator.create_organization(how_many=20)

    logger.info("Step 2 - Generate Emission Sources")
    data_generator.create_emission_sources()

    logger.info("Step 3 - Create Emission Factors")
    data_generator.create_emission_factors()

    logger.info("Step 4 - Create Emissions Logs")
    data_generator.create_emission_logs(how_many=1000)


if __name__ == "__main__":
    wait_for_db()  # Wait for DB to be ready
    generate_data()  # Proceed with data generation
