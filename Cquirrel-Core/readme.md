# Operation Guide

**Attention**: Your local machine must have Maven 3.9+ and JDK 21+ installed.  
You can install JDK and Maven using SDKMAN.

1. **Package the Java Code**: Run the following command to package the Java code:
   ```bash
   mvn clean package
   ```
2. Run the Java Application: Execute the following command to run the Java application. This will generate the query results in ./../Cquirrel-Script/data/output.csv and the running logs in the current directory:
   ```bash
   time java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED -jar target/Cquirrel-Core-1.0-SNAPSHOT.jar > output.txt
   ```
3. Check the Results: Navigate to the Cquirrel-Script directory and run the script to validate the results:
   ```bash
   cd Cquirrel-Script
   python data_check.py
   ```