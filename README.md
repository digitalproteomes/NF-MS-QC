# MS-QC Workflow

This Nextflow pipeline implements a Mass Spectrometry Quality Control (MS-QC) workflow. It processes raw mass spectrometry data, performs protein identification using FragPipe, and generates interactive quality control reports using Jupyter Notebooks.

## Pipeline Overview

1. **Raw File Conversion**: Converts Thermo raw files to mzML format using `NF-ConvertThermo`.
2. **FragPipe Search**: Generates a manifest file and runs FragPipe for protein identification/PSM reporting.
3. **Report Generation**: Executes a parameterized Jupyter Notebook via `papermill` to generate QC reports.
4. **HTML Conversion**: Converts the generated Jupyter Notebooks to HTML format for easy viewing.

## Requirements

- [Nextflow](https://www.nextflow.io/)
- [FragPipe](https://fragpipe.nesvilab.org/)
- [Python](https://www.python.org/) with `papermill` and `jupyter` installed
- Java (for Nextflow)

## Usage

To run the pipeline, execute the following command:

