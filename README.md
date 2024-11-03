# Data Processing

This is a web application that processes and displays data, focusing on data type inference and conversion for datasets using Python and Pandas.

## Project Overview

This project is a web application designed to automate data type inference and conversion for uploaded datasets. The app processes data from CSV or Excel files and displays it with inferred data types.

Data types handled include:
- object
- int64, int32, etc.
- float64, float32
- bool
- datetime64
- category
- complex

## Features

- **Data Type Inference and Conversion**: A backend script built with Pandas detects the most appropriate data type for each column.
- **Error Handling and Validation**: Comprehensive validation ensures proper data processing.
- **User-Friendly Interface**: Users can upload CSV/Excel files and view processed data.
- **Performance Optimizations**: The backend is optimized for large datasets, ensuring efficient memory use and processing speed.

## Tech Stack

- **Frontend**: React
- **Backend**: Django, Django REST framework
- **Data Processing**: Pandas (Python)

---

## Getting Started

### Prerequisites

- **React.js** (for frontend): [Download here](https://react.dev/)
- **Node.js**: [Download here](https://nodejs.org/)
- **Python** (for backend): [Download here](https://www.python.org/)
- **Django** and **Django REST framework**(https://www.djangoproject.com/)
- **Pandas** for data processing(https://www.python.org/)

### Installation

1. Clone the repository:

    ```bash

    git clone https://github.com/coding-cooking/data_processing_fe
    cd data_processing_fe

    git clone https://github.com/coding-cooking/data_processing_be
    cd data_processing_be
    ```

2. Set up frontend and backend as described in the following sections.

---

## Frontend

The frontend is a React application, providing the user interface for the project.

### Frontend Setup

1. Navigate to the frontend directory:

    ```bash
    cd data_processing_fe
    ```

2. Install dependencies:

    ```bash
    npm install
    ```

3. Start the frontend server:

    ```bash
    npm start
    ```

The app should now be running on [http://localhost:3000](http://localhost:3000).

### Frontend Available Scripts

- **`npm start`**: Runs the app in development mode.
- **`npm run build`**: Builds the app for production.
- **`npm test`**: Runs the test suite.

### Frontend Project Structure

- **`src/App.js`**: The main app component.

---

## Backend

The backend is built with Django and Django REST framework, providing the API and data processing for the project.

**Backend Setup**:

- Navigate to the backend folder:
        ```bash
        cd data_processing_be
        ```
    - Set up a virtual environment:
        ```bash
        python -m venv .venv
        source .venv/bin/activate  # On Windows: venv\Scripts\activate
        ```
    - Install dependencies:
        ```bash
        pip install -r requirements.txt
        ```
    - Run database migrations:
        ```bash
        python manage.py migrate
        ```
    - Start the Django server:
        ```bash
        python manage.py runserver
        ```

The backend server should now be running on [http://localhost:8000](http://localhost:8000).

## Usage

1. **Upload a CSV or Excel file**: Use the frontend to upload your dataset.
2. **View Processed Data**: The app infers data types for each column, displaying the dataset with detected types.
3. **Manual Override**: If desired, manually override data types from the frontend interface.

## API Documentation

The backend API provides the following endpoints:

- **`POST /process/`**: Upload a file for processing.
    - **Request**: File upload (CSV or Excel)
    - **Response**: JSON with inferred data types and dataset preview.

---

### Backend Project Structure

- **`data_processing/`**: The main Django project directory containing configuration files, including settings, URLs, etc.
- **`data_processor/`**: A Django app within the data_processing project that contains the core functionality for data processing. 
- **`manage.py`**: Django management script for running tasks.
- **`data_processor/utils/`**: Any helper functions or data processing scripts (e.g., for Pandas processing).
- **`requirements.txt`**: Lists all Python dependencies.

---

## Challenges and Considerations

Automating data cleaning is inherently challenging due to the uniqueness of datasets. Specific issues include:

- **Mixed Data Types**: Columns with different data types may default to `object`. This app uses customized inference to resolve such ambiguities.
- **Non-Standard Formats**: Data may appear in unconventional formats, requiring additional handling.
- **Large File Processing**: Optimizations ensure minimal memory usage and faster processing.

The Python script `utils.py` is designed to address these issues while balancing performance and accuracy.

---

## Contributing

1. Fork the repository.
2. Create a branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a Pull Request.