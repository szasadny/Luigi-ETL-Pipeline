from luigi import server
from luigi import scheduler

# Configure the Luigi server
api_port = 8082 
scheduler = scheduler.Scheduler()

# Start the Luigi server
if __name__ == '__main__':
    server.run(api_port=api_port, scheduler=scheduler)