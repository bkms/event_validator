# Event validation, ingestion and monitoring pipeline

## <b>Notes on Tasks</b>

<b>Task 1</b> - Please see `pipeline.py` for solution to this

<b>Task 2 - Monitoring</b>
    
<b>A) Integrate (or implement it independently) a mechanism that checks if all the data in the file has been properly stored</b>

Currently, I just do this via exception handling on the database insert transaction. However, I could have a separate process
that runs to just reconcile the contents of the database table with the input file (minus invalid lines) using a hash of each
line's concatenated values. Otherwise, I could add `return hash(concat(field1,field2..))` on the end of my INSERT sql and perhaps
ensure the correct values are persisted that way.

<b>B) Can you think of any different aspects of the pipeline that would be worth monitoring?</b>
    
If the pipeline is run on a distributed computing system (like, say, Apache Flink) then we would want to know:
- CPU and memory utilisation of the VM's/cluster
- Throughput (events per second) and latency (how long does a process need to wait on network/db issues at 50th/90th percentiles)
- The size, growth rate and trends in the error bucket/table/file
- Load on the database, both in terms of network connections and table size


<b>3. CI/CD - Describe from a high level the different steps you would implement to transition the pipeline from a development environment into a production one</b>

I would follow a fairly standard procedure;
- We have a `dev`, `test` and `prod` environment
- Devs would branch off from main, implement their change, raise a PR and have it reviewed by several code owners/peers. Naming of branches/commits/etc would follow gitflow & semver to ensure traceability later and to automate versioning operations. (ie feature branches vs hotfixes vs breaking changes named accordingly etc)
- Possibly have smoke tests automatically kick off via githooks upon raising of PR to uncover major problems early
- When a PR is approved, the CI tool would initiate automated unit testing and security analysis (SCA/SAST) and merge back into main branch if successful
- Upon an update to the main branch, the CI would auto promote this updated version to the test environment for further integration and end to end testing with more data
- Once testing is completed, the tester would raise another PR to merge the prod release candidate to the release branch. CI would auto increment the semantic versioning to reflect the nature of the changes and then deploy it to prod environment.



<b>4. (Optional:) Bonus points if everything is dockerised. From a high level, how would you deploy it in Kubernetes?</b>

The `Dockerfile` and `docker-compose.yml` are included in the `.devcontainer` folder.

To be deployed in a Kubernetes pod, it will need some work in setting up a proper network connection between the Python running container and the database. Currently this is just set to default `localhost`. We'd then need to specify the deployment details such as replicas, labels, ports etc in the `deployment.yaml` file. Then we would `kubectl apply -f deployment.yaml` onto our K8s cluster. 

## <b>How to Run</b>

I've developed this solution in VS Code dev containers; a Docker composed setup with one container to run Postgres and another to run the Python script. I have included the `.devcontainer` utility directory in the repo in case you want to easily run it in VS Code yourself.

Simply clone the repo and install the VS Code dev container extension. The extension will show you a `REMOTE EXPLORER` tab on the left hand panel, you can navigate into that and follow the instructions to open the repo in a new dev container (which will inherit its settings from the `.devcontainer` directory spec files)


