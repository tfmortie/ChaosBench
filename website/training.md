# Training

> __NOTE__: Hands-on modeling and training workflow can be found in `notebooks/02a_s2s_modeling.ipynb` and `notebooks/03a_s2s_train.ipynb`

We will outline how one can implement their own data-driven models. Several examples, including ED, FNO, ResNet, and UNet have been provided in the main repository. 

### Step 1
Define your model class in `chaosbench/models/<YOUR_MODEL>.py`

### Step 2
Initialize your model in `chaosbench/models/model.py` under `S2SBenchmarkModel.__init__`

### Step 3
Write a configuration file in `chaosbench/configs/<YOUR_MODEL>_s2s.yaml`. Details on the definition of [hyperparameters](https://leap-stc.github.io/ChaosBench/baseline.html) and the different [task](https://leap-stc.github.io/ChaosBench/task.html). Also change the `model_name: <YOUR_MODEL>_s2s` to ensure  correct pathing

- Task 1️⃣ (autoregressive): `only_headline: False ; n_step: <N_STEP>`
- Task 1️⃣ (direct): `only_headline: False ; n_step: 1 ; lead_time: <LEAD_TIME>`

- Task 2️⃣ (autoregressive): `only_headline: True ; n_step: <N_STEP>`
- Task 2️⃣ (direct): `only_headline: True ; n_step: 1 ; lead_time: <LEAD_TIME>`

    
### Step 4
Train by running `python train.py --config_filepath chaosbench/configs/<YOUR_MODEL>_s2s.yaml`  


### Note
Remember to replace `<YOUR_MODEL>` with your own model name, e.g., `unet`. Checkpoints and logs would be automatically generated in `logs/<YOUR_MODEL>_s2s/`.