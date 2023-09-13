# Air Pressure System (APS) FAILURE PREDICTION

### Problem Statement
The Air Pressure System (APS) is an important component in heavy-duty vehicles that applies pressure to the brake pads and slows the vehicle. APS failures can have serious consequences, such as compromised vehicle safety. Predicting these failures in advance can help with timely maintenance and avoid potential disasters. 

The goal of this binary classification problem is to determine whether a failure was caused by an APS component or by something else.The positive class in the dataset corresponds to component failures for a specific APS system component. Trucks in the negative category have failed components not related to the APS system.The goal is to reduce the cost of unnecessary repairs, so the number of incorrect predictions must be reduced.

## Tech Stack Used
1. Python 
2. FastAPI 
3. Machine learning algorithms
4. Docker
5. MongoDB

## Infrastructure Required.

1. AWS S3
2. AWS EC2
3. AWS ECR
4. Github Actions
5. Terraform


## Data Collections
![image](images\data_collection.png)


## Project Archietecture
![image](images\overall_archietecture.png)


## Deployment Archietecture
![image](images\deployment.png)


### Step 1: Clone the repository
```bash
git clone https://github.com/sethusaim/aps-Fault-Detection.git
```

### Step 2- Create a conda environment after opening the repository

```bash
conda create -n env python=3.7.6 -y
```

```bash
conda activate env
```

### Step 3 - Install the requirements
```bash
pip install -r requirements.txt
```

### Step 4 - Export the environment variable
```bash
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>

export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>

export AWS_DEFAULT_REGION=<AWS_DEFAULT_REGION>

export MONGODB_URL= MONGO_URL

```

### Step 5 - Run the application server
```bash
python app.py
```

### Step 6. Train application
```bash
http://localhost:8080/train

```

### Step 7. Prediction application
```bash
http://localhost:8080/predict

```

## Run locally

1. Build the Docker image
```
docker build --build-arg AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID> --build-arg AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> --build-arg AWS_DEFAULT_REGION=<AWS_DEFAULT_REGION> --build-arg MONGODB_URL=<MONGODB_URL> . 

```
2. Run the Docker image
```
docker run -d -p 8080:8080 <IMAGE_NAME>
```

To run the project  first execute the below commmand.

```bash
export MONGO_DB_URL=MONGO_URL
```
then run 
``` bash
python main.py
```