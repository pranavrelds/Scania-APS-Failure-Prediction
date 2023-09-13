terraform {
  backend "s3" {
    bucket = "aps-tf-state"
    key    = "tf_state"
    region = "ap-south-1"
  }
}

module "aps_ec2" {
  source = "./aps_ec2"
}

module "aps_model" {
  source = "./aps_model_bucket"
}

module "aps_ecr" {
  source = "./aps_ecr"
}

module "aps_pred_data" {
  source = "./aps_pred_data_bucket"
}