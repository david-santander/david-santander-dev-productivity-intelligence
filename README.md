# Instructions for Setting Up Gitea, SonarQube, and Jenkins

## Docker Instantiation

1. **Navigate to the project directory:** Open your terminal and navigate to the directory containing the `docker-compose.yml` file.
2. **Run Docker Compose:** Execute the following command to start the services defined in the `docker-compose.yml` file:

    ```bash
    docker-compose up -d
    ```

    This command will start the Gitea, SonarQube, and Jenkins services in detached mode.

## Gitea Setup

1. **Access Gitea:** Open your web browser and go to `http://localhost:3000`.
2. **Initial Setup:** Follow the on-screen instructions to set up the Gitea instance. Create an admin user.
3. **Create a Sample User:**
    * Log in as the admin user.
    * Create a new user (e.g., `admin`).
4. **Create a Sample Repository:**
    * Log in as the `admin`.
    * Create a new repository named `django-sample-app`.
    * Initialize the repository with a README file.
5. **Push the Python/Django Application Code:**
    * Navigate to the `django-sample-app` directory in your terminal.
    * Run the following commands to push the code to the Gitea repository:

        ```bash
        git init
        git checkout -b main
        git add .
        git commit -m "first commit"
        git remote add origin http://localhost:3000/admin/django-sample-app.git
        git push -u origin main
        ```

6. **Configure a Gitea Webhook:**
    * Go to the `django-sample-app` repository settings in Gitea.
    * Click Webhooks on the leftside bar, then click Add Webhook -> Gitea.
    * Add a webhook that triggers on `push` events to the `main` branch.
    * Set the webhook URL to your Jenkins instance (e.g., `http://jenkins:8080/generic-webhook-trigger/invoke?token=mySuperSecretGiteaHookToken123!`).
    * Set the Content type to `application/json`.
7. **Generate a Gitea Access Token:**
    * Go to the `admin` settings in Gitea.
    * Create a new access token with the name `Jenkins Access Token`.
    * Repository and Organization Access: All.
    * Permissions: Read and Write.
    * Copy the generated token. You will need this token to configure Jenkins.

## SonarQube Setup

1. **Access SonarQube:** Open your web browser and go to `http://localhost:9000`.
2. **Initial Setup:** Follow the on-screen instructions to set up the SonarQube instance. Create an admin user.
    * Login with user: admin and pwd: admin
    * Update password: Admin123456!
3. **Generate a SonarQube Authentication Token:**
    * Log in as the admin user.
    * Go to My Account.
    * Go to Security.
    * Generate a new token as User Token with no-expiration.
    * Copy the generated token. You will need this token to configure Jenkins.
4. **Create a SonarQube Project:**
    * While not strictly necessary (the scanner can create the project), it's good practice to pre-create the project.
    * Click "Add project" and select "Manually".
    * Enter the project display name `dev.productivity:django-sample-app`.
    * Enter the project key `dev.productivity:django-sample-app`.
    * Enter the main branch name `main`.

## Jenkins Setup

1. **Access Jenkins:** Open your web browser and go to `http://localhost:8080`.
2. **Initial Setup:** Follow the on-screen instructions to set up the Jenkins instance. Install the suggested plugins.
    * Execute docker-compose logs jenkins to get the admin pwd.
    * Install suggested plugins.
    * Create the Admin user.
    * Instance Configuration Jenkins URL: `http://localhost:8080/`.
3. **Install Required Plugins:**
    * Go to "Manage Jenkins" -> "Manage Plugins".
    * Install the following plugins:
        * SonarQube Scanner
        * Gitea Plugin
        * Generic Webhook Trigger Plugin
        * Credentials Plugin
    * Restart Jenkins
4. **Configure Credentials:**
    * Go to "Manage Jenkins" -> "Security" -> "Credentials".
    * Go to "Stores scoped to Jenkins" -> "System" -> "Global Credentials (unrestricted)".
    * Click on: Add Credentials.
    * Create two new credentials:
        * **Gitea Token:**
            * Kind: `Gitea Personal Access Token`
            * Scope: `Global`
            * ID: `gitea-token`
            * Secret: Paste the Gitea access token you generated in the Gitea setup steps.
        * **SonarQube Token:**
            * Kind: `Secret text`
            * Scope: `Global`
            * ID: `sonarqube-token`
            * Secret: Paste the SonarQube authentication token you generated in the SonarQube setup steps.
5. **Create a Jenkins Pipeline Job:**
    * Go to "New Item".
    * Enter a name for the pipeline job (e.g., `django-sample-app-pipeline`).
    * Select "Pipeline" and click "OK".
    * In the pipeline configuration:
        * Select Generic Webhook Trigger
        * Create a variable with name `BRANCH_NAME`
        * Add the expresssion `$.ref`
        * Select JSONPath
        * Enter a strong, unique token. For example: mySuperSecretGiteaHookToken123!.
        * Select "Pipeline script from SCM".
        * SCM: Git
        * Repository URL: `http://gitea:3000/test-user/django-sample-app.git`
        * Credentials ID: `gitea-token`
        * Branch: `*/main`
        * Script path: `Jenkinsfile`
    * Save the pipeline job.
6. **Configure Jenkins to use the SonarQube Scanner:**
    * Go to "Manage Jenkins" -> "Tools".
    * Add a SonarQube Scanner installation.
    * Name: Sonarcanner
    * Version: (Choose the latest version)
    * Check "Install automatically"
    * Click Save
7. **Configure Jenkins to use the SonarQube Server:**
    * Go to "Manage Jenkins" -> "System".
    * Add a SonarQube Servers.
    * Name: SonarQubeServer
    * Server URL: <http://sonarqube:9000>
    * Server authentication token: `sonarqube-token`
    * Click Save

## Trigger the Pipeline

1. **Commit and Push Changes:** Make a change to the Python/Django application code and commit the changes to the `main` branch of the Gitea repository.
2. **Verify the Pipeline Execution:** Go to the Jenkins pipeline job and verify that the pipeline is triggered automatically by the Gitea webhook.
3. **View SonarQube Results:** Once the pipeline has completed successfully, go to the SonarQube dashboard (`http://localhost:9000`) and view the analysis results for the `dev.productivity:django-sample-app` project.
