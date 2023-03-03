from diagrams import Diagram
from diagrams import Cluster, Edge, Node
from diagrams.azure.identity import Users
from diagrams.onprem.container import Docker
from diagrams.onprem.workflow import Airflow
from diagrams.aws.storage import SimpleStorageServiceS3 as S3
from diagrams.oci.monitoring import Notifications
from diagrams.azure.general import Helpsupport
from diagrams.gcp.operations import Monitoring
from diagrams.aws.management import Cloudwatch
from diagrams.aws.storage import ElasticBlockStoreEBSSnapshot
from diagrams.aws.compute import EC2
from diagrams.aws.database import Database

with Diagram("Architecture Diagram", show=False):
    ingress = Users("Users")
    with Cluster(""):
        with Cluster("EC2"):
          with Cluster("Application"):
              streamlit = Docker("Streamlit")
              backend = Docker("FastAPI")
          ec2=EC2("EC2 Instance")
        with Cluster("Sqlite Database"):
            db = Database("SQlite")
        with Cluster("AWS services"):
            cloudwatch = Cloudwatch("AWS Logs")
            bucket = ElasticBlockStoreEBSSnapshot("AWS s3 bucket")
        with Cluster("Airflow Process"):
            airflow = Airflow("Airflow")
            Great_Expectations_Reports = Notifications("Meta-Data Check")

    
    
    backend << Edge(label="API Call", color="red") << streamlit
    streamlit << Edge(label="API Response", color="red") << backend
    db << Edge(label="Data Fetch") << backend
    ec2 << Edge (label = "Requests") << ingress
    streamlit << Edge(label="Fetch Page", color="red") << ec2
    metadata = S3("NOAA and GEOS Bucket")
    Great_Expectations_Reports>> Edge(label="Reports for Great Expectations reports") >> bucket
    backend<< Edge(label="Fetch data for reporting and logs ")  << cloudwatch

    Great_Expectations_Reports >> Edge() >> db
    airflow >> Edge(label="GE checkpoints") >> Great_Expectations_Reports

    airflow << Edge(label="Data Collection") << metadata