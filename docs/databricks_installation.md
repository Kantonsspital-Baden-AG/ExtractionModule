# Databricks Installation Guide

To install the package in a databricks cluster, one first needs to clone the git repo and then install the wheel file found in the package.

## Clone the Extraction Module repository

1. Navigate to the your workspace;
2. From the top right panel select `Create/Git folder`;
3. populate the encessary field with the specifics of the package;

## Install the wheel file

1. Navigate to the workspace;
2. On the left panel, select `Compute`;
3. Select the compute you would like to install the package on, and navigate the `Libraries` tab;
4. On the right side of the window, select `Install new`;
5. Select `Workspace` in the `Library Source` menu;
6. From the `Workspace File Path` select the `ExtractionModule` folder and select the file `dist/extraction-**-.whl`;
7. Wait until installation is done;
