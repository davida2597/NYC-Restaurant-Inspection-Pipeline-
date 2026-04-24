# NYC Restaurant Inspection Analysis Dashboard

## Project Overview

This project analyzes restaurant inspection data from the **New York City Department of Health and Mental Hygiene (DOHMH)** to identify patterns and potential trends for restaurant inspection outcomes.

The core goal of this project is to provide an **interactive dashboard** that allows users to explore the inspection dataset visually and uncover trends using filters and data exploration tools.

The dataset used in this project comes from the NYC Open Data portal API:

https://data.cityofnewyork.us/d/43nn-pn8j

---

## Objectives

This project investigates several questions related to restaurant inspections in New York City:

- Are restaurant inspections stricter in certain boroughs or neighborhoods?
- Is there a relationship between restaurant **inspection scores** and **customer reviews**?
- Do areas with a **high density of restaurants** experience different inspection outcomes?
- Are there geographic clusters of restaurants with consistently higher or lower inspection scores?

Rather than presenting fixed conclusions, the project focuses on building an **interactive dashboard** that enables users to explore these relationships visually.

---

## Dataset

The primary dataset is the **DOHMH New York City Restaurant Inspection Results** dataset.

It includes information such as:

- Restaurant name
- Cuisine type
- Borough
- Address
- Inspection date
- Inspection grade (A, B, C)
- Violation codes
- Inspection score
- Geographic location

These records provide insight into how restaurants perform during health inspections across New York City.

---

## Data Processing Pipeline

The general workflow of the project is:

1. **Data Collection**
   - Retrieve the inspection dataset from NYC Open Data.

2. **Data Cleaning**
   - Remove duplicates
   - Handle missing values
   - Standardize location and restaurant identifiers
   - Filter relevant inspection types

3. **Feature Engineering**
   - Aggregate inspections by restaurant
   - Compute average inspection scores
   - Calculate restaurant density by region

4. **Data Visualization**
   - Build an interactive dashboard for exploration.

---

## Dashboard Features

The dashboard allows users to interactively explore the data through:

- **Geographic maps** showing restaurant inspection scores
- **Filters by borough and cuisine type**
- **Inspection score distributions**
- **Restaurant density analysis**
- **Trend exploration over time**
- **Comparisons between regions**

These visualizations allow users to identify patterns such as:

- Areas with consistently high or low inspection scores
- Regions with a high concentration of restaurants
- Potential relationships between inspection performance and location

---

## Technologies Used

This project uses the following main tools:

- **Python**
- **Supabase** - cloud file and database storage
- **Github Actions** - autonomous pipeline updates
- **Psycopg 3** - supabase connector
- **Pandas** – data cleaning and manipulation
- **Streamlit** – interactive dashboard
- **Streamlit Community Cloud** - cloud accessible dashboard environment
- **NYC Open Data API**

---

## How to use

This project is continous, meaning it will continue to work until canceled. 

To access the interactive dashboard querying our data, go to https://nyc-restaurant-inspection-dashboard.streamlit.app/

## Contributors

David Arnold, Erica Chen, Zarko Dimitrov, and Arnav Karnati.
