import click
import requests
import typer
import getpass

app = typer.Typer()

user_bucket_name = "damg-test"
subscription_tiers = ["free", "gold", "platinum"]
BASE_URL = "http://127.0.0.1:8000"
access_token = ""
headers = {"Authorization": f"Bearer {access_token}"}



@app.command("Server_Health")

def health_check():
    # import ipdb ; ipdb.set_trace()

# Prompt the user for server link
    health = typer.prompt("Enter the URL for the Application")

    url= BASE_URL
    url_response= requests.post(url)


    if url_response.status_code == 405:
        typer.echo("Server is up and Running")
    else:
        typer.echo("Internal Server Issue")



@app.command("create_user")
def create_user():
    # Prompt the user for username
    username = typer.prompt("username")
    password = typer.prompt("password")
    password2 =typer.prompt("Reenter password")
    tier = typer.prompt("tier")

    url = "{0}/{1}".format(BASE_URL, "register")

    body = {"username": username, "password": password, "tier": tier}
    username_response = requests.post(url, json=body)

    # typer.echo (username_response.text)
    if username_response.status_code == 201:
        typer.echo("User created")



@app.command("forgot_password")
def create_user():
    # Prompt the user for username
    username = typer.prompt("username")
    password = typer.prompt("password")
    tier = typer.prompt("tier")

    url = "{0}/{1}".format(BASE_URL, "forgot_password")

    body = {"username": username, "password": password, "tier": tier}
    username_response = requests.post(url, json=body)

    # typer.echo (username_response.text)
    if username_response.status_code == 201:
        typer.echo("Password Updated")





@app.command("login")
def user_login():

     # Prompt the user for username
    username = typer.prompt("username")
    password = typer.prompt("password")
    tier = typer.prompt("tier")

    url = "{0}/{1}".format(BASE_URL, "login")

    response = requests.get(url)

    if response.status_code == 200:
       typer.echo("User Logged in  Successfully")



@app.command("goes_get_year")
def user_login():

     # Prompt the user for Year
    get_year = typer.prompt("Enter The year")
    

    url = "{0}/{1}".format(BASE_URL, "geos_get_year")

    response = requests.get(url)

    if response.status_code == 200:
       typer.echo("The Files has been fetched")

       


if __name__ == "__main__":
    app() 