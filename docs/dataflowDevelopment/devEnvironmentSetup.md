Setting up your development environment
=======================================

1. Pull the latest NeverwinterDP code
        
        git clone http://github.com/Nventdata/NeverwinterDP

2. Choose your branch.  The master branch will have the lastest stable code.  Other branches will have code in development
        
        cd NeverwinterDP
        git checkout dev/master

3. Create your eclipse project files, build, and release

        cd NeverwinterDP
        ./gradlew clean build install eclipse -x test

4. Import the project into Eclipse
  - Open Eclipse
  - File -> Import -> Existing Project into Workspace
  - Then navigate to the NeverwinterDP folder and import the project
5. Get coding!
