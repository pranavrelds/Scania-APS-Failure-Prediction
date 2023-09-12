# scania-APS-Failure-Prediction

## Problem Statement
The Air Pressure System (APS) is a vital component in heavy-duty vehicles, using compressed air to apply pressure to brake pads, slowing the vehicle. Failures in the APS can lead to severe consequences, including compromised vehicle safety. Predicting these failures in advance can help in timely maintenance and prevention of potential mishaps. The challenge is to determine whether a failure was caused by a specific APS component or by another factor.

## Solution
This repository offers a solution to predict APS failures using machine learning. The model is trained on the `aps_failure_training_set1.csv` dataset, which contains various features related to the APS system and the target variable indicating the type of failure.

- **Training**: The `train.py` script initializes and runs the training pipeline, producing a model capable of predicting APS failures.

- **Testing & Prediction**: The repository also includes scripts for testing the model (`test.py`) and making predictions (`prediction.py`).
