# Use the latest gcc image as the base
FROM gcc:latest

# Set the working directory in the container
WORKDIR /Membership

# Copy the current directory contents into the container
COPY . .

# Compile the C++ program with C++11 standard and pthread support
RUN g++ -std=c++11 -o membership main.cpp -pthread

# Set the entry point to your application
ENTRYPOINT ["./membership"]

# Use CMD to provide default arguments (overrideable in docker-compose)
CMD []
