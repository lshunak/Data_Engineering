# Use the latest Ubuntu image as the base
FROM ubuntu:latest

# Create a non-root user named 'labuser'
RUN useradd -ms /bin/bash labuser

# Set the working directory
WORKDIR /home/labuser

# Copy the Bash script into the container
COPY colleagues.sh .

# Make the script executable
RUN chmod +x colleagues.sh

# Change ownership of the script to the non-root user
RUN chown labuser:labuser colleagues.sh

# Switch to the non-root user
USER labuser

# Set the script to run by default when the container starts
CMD ["./colleagues.sh"]
