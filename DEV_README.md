#Code Convention#

1. You need to follow the code convention of the language you are using

2. Configure  your IDE properly for indentation, space replacement

3. Give the class, method, variable meaningful name. Use the proper abbreviation rule if you want to shorter the name.

#Commit Code#

1. Before commit and check in your code,  You need to make sure that the code should not create a major failure or problem. Which mean you have to know the statusof your code, know if your current code cause any test fail at least in the project that you are modifying

2. When you check in the code ,  you always have to check in from root which mean you either checkin all or none.  You are not allow to commit partial code.

3. Always run git status after check in to make sure you do not miss any resources to check in

4. Always read the messages and know the status. In many cases like you run the push command and think you already push the change, but there is a conflict in the remote repository and it does not allow you to push before you resolve the conflict.


#Development Rule#

1. When you work with a large project code, you should classify the code into different category like util , core api , service, debug , ui... If  you can identify the code and classify the code, you will see that util , core , api, service... usually are the critical code and you should be alert when modify the critical code. You can ask , discuss with the other before you change the core code. On the other hand , the core code usually should be well thought and support what you need. So if you look for a solution by add a feature or modify the core code for what you need, usually will be a bad solution or the one who develop the core code is a bad designer or developer. There is always a reason that the designer do not include a feature or expose some data to you. The reason in most case relate to performance and security.

2. Before you modify a code,  you should know the testing status of the project, how many test pass how many test fail. What are the exceptions you see when you run the test. So if you modify some code in a project , you will realize the the effect of  your code change , does it create new  test fail , exceptions...

3. When you modify a piece of code , you should know that modification will cause a local effect or global effect. It is easy by searching to see how many classes are depended on the code , class that you modify. Less code depend on your modified code , the less dangerous  your modification is. So always look for a local solution before think to a global solution. Especially the code is new to you,  you are not a senior yet. If you have a global solution , that you think it will benefice for the project, you can always discuss , tell the other about your idea and you will gain more credit from the team and everybody learn your idea, as it is a good pattern or solution.

4. When you encounter a problem and you need to ask, you have to understand that problem, collect all the information such log message , exception. Describe your problem with the collect data as short and easy to understan as possible. Let take an example:

* You run into a problem and you post the question say that you run the unit test xyz fail with 1M log file and ask for help.

* You run into a problem , you describe the problem with your input data, the root exception that you encounter

  Which way do you think you have the chance to get a help.

5. The final goal is to work efficient , produce a good quality code. The goal of the task tracker is to help to manage and communicate. If you think that you enter more tasks into the task trackers and it look like that you are more productive since you can solve more tasks. But what if I say the more tasks you enter the more bad works you have , as you task has more bugs and you have to fix , rework... The final good code you have is the one who review , who find the problem for you or suggest a solution. What do you think.

The less tasks you have in the tracker, the less work for you , for the tester , for the manager to review. So you should learn how to enter just enough to help communication and management or organize your work priority... If you take the goal as you can solve as much issues in the tracker you will never be a good developer or manager , as you have no time to learn the code.

6. You should do or work on what you understand and you see it makes sense. You should not work on some thing because the manager ask. If you see the request does not make sense in your understand you have to ask until you understand or refuse that task or explain back to the manager that the request doesn't make sense.
