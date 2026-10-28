@echo off
echo ========================================================
echo    Building KTP (KGP Transfer Protocol) Java Project
echo ========================================================
echo Compiling Java source files...
javac -g *.java
if %ERRORLEVEL% NEQ 0 (
    echo Compilation failed!
    exit /b %ERRORLEVEL%
)
echo Creating JAR archive ktp.jar...
jar cfe ktp.jar InitKSocket *.class
echo Generating test files...
java -cp . GenerateTestFile
echo.
echo Build successful!
echo You can run the programs using:
echo   1. Start daemon:    run_init.bat   (or java -cp ktp.jar InitKSocket)
echo   2. Start receiver:  run_user2.bat  (or java -cp ktp.jar User2 output.txt)
echo   3. Start sender:    run_user1.bat  (or java -cp ktp.jar User1 input.txt)
echo   4. Run evaluation:  java -cp ktp.jar EvaluateP input.txt
