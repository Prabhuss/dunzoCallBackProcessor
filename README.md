Dunzo Callback Processor
This repository contains code for a Dunzo Callback Processor, which is responsible for processing callback messages from the Dunzo delivery service. The code is written in C# and is designed to be executed as a WebJob in Microsoft Azure.

Prerequisites
Before running the Dunzo Callback Processor, make sure you have set the following connection strings in the app.config file:

AzureWebJobsDashboard: Connection string for the Azure WebJobs dashboard.
AzureWebJobsStorage: Connection string for Azure WebJobs storage.
Getting Started
To get started with the Dunzo Callback Processor, follow these steps:

Clone the repository: git clone https://github.com/your-username/dunzo-callback-processor.git
Open the solution in Visual Studio.
Set the connection strings in the app.config file.
Build the solution to ensure all dependencies are resolved.
Usage
The entry point of the application is the Program.cs file, which contains the Main method. This method sets up the JobHost configuration and starts the continuous execution of the WebJob.

The processing logic for the Dunzo callback messages is implemented in the Function.cs file. The ProcessQueueMessage method is triggered whenever a new message is written to the Azure Queue called "dunzocallbackq". Inside this method, the callback message is processed and various operations are performed based on the message content.

To customize the processing logic, you can modify the code in the ProcessQueueMessage method and the related helper methods in the Functions class.

Dependencies
The Dunzo Callback Processor relies on the following dependencies:

Microsoft.Azure.WebJobs (version X.X.X): This library provides the necessary components for building and running WebJobs in Azure.
Microsoft.WindowsAzure.Storage (version X.X.X): This library is used for interacting with Azure storage services.
Newtonsoft.Json (version X.X.X): This library is used for JSON serialization and deserialization.
Make sure these dependencies are resolved correctly and up to date.

License
This project is licensed under the MIT License. Feel free to modify and distribute the code as per your requirements.
