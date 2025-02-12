import pandas as pd

# Create the synthetic data
data = {
    "Number": ["INC543210", "INC785312", "INC912345", "INC134567", "INC278901", 
               "INC390874", "INC501293", "INC617854", "INC729384", "INC845623"],
    "Opened": ["2024-08-10 12:30:45", "2024-07-22 08:10:30", "2024-06-15 15:45:10", "2024-05-30 10:20:05", 
               "2024-04-12 14:05:50", "2024-03-22 09:55:35", "2024-02-10 16:25:40", "2024-01-05 11:45:00", 
               "2023-12-18 08:30:15", "2023-11-30 17:15:45"],
    "Short description": ["User unable to log in", "Slow network issue", "Laptop not turning on", 
                          "VPN connection dropping", "Printer not responding", "Unauthorized access attempt", 
                          "Software license expired", "Email delivery failure", "New employee access request", 
                          "System performance degraded"],
    "Priority": ["High", "Medium", "Critical", "High", "Low", "Critical", "High", "Medium", "Low", "High"],
    "State": ["Open", "Resolved", "In Progress", "Open", "Closed", "Resolved", "Pending", "In Progress", "On Hold", "Resolved"],
    "Issue Category": ["Software", "Network", "Hardware", "Network", "Hardware", "Security", "Software", "Network", "Access Issue", "Software"],
    "Assignment group": ["IT Support", "Network Team", "IT Support", "Network Team", "IT Support", "Security Operations", 
                         "IT Support", "IT Support", "HR Support", "IT Support"],
    "Assigned to": ["John Doe", "Alex Johnson", "Michael Scott", "Maria Lopez", "Jake Thompson", "Lisa Wong", 
                    "Robert King", "Emma Davis", "", "Sarah Collins"],
    "Updated": ["2024-08-12 09:15:20", "2024-07-22 12:50:45", "2024-06-16 11:30:25", "2024-05-30 16:10:30", 
                "2024-04-13 09:50:10", "2024-03-23 12:30:20", "2024-02-11 08:20:55", "2024-01-05 14:50:25", 
                "2023-12-19 09:10:30", "2023-12-01 09:35:20"],
    "Updated by": ["Jane Smith", "Sarah Lee", "David Miller", "Steve Carter", "Jake Thompson", "Kevin Brown", 
                   "Megan White", "John Carter", "Richard Hall", "James Wilson"],
    "Parent incident": ["INC123456", "", "", "", "", "", "", "INC489321", "", ""],
    "Actions taken": ["Reset password", "Restarted router and switched to backup", "Diagnosed power issue", 
                      "Investigating firewall settings", "Replaced printer driver", "Blocked suspicious IP address", 
                      "Requested license renewal", "Restarted mail server", "Processing request", "Increased server resources"],
    "Comments and work notes": ["User unable to log in due to error.", "Network latency issues reported by users.", 
                                "User reported sudden shutdown.", "Users experiencing VPN disconnects.", 
                                "Printer issue resolved after update.", "Incident logged with security team.", 
                                "Awaiting approval from procurement.", "Issue affecting outgoing emails.", 
                                "Awaiting HR approval.", "System slowdowns reported by users."],
    "Description": ["Login issue affecting multiple users", "Network delay in multiple locations", 
                    "Suspected motherboard failure", "VPN issue reported by remote employees", 
                    "Print jobs were getting stuck", "Unauthorized login attempts detected", 
                    "Users unable to access licensed software", "Some users unable to send emails", 
                    "Employee needs access to multiple systems", "High memory usage observed"],
    "Resolution notes": ["", "", "", "", "", "Account flagged and secured", "", "", "", "Issue resolved after tuning"],
    "Resolved": ["", "2024-07-22 14:00:00", "", "", "2024-04-13 10:00:00", "2024-03-23 13:00:00", "", "", "", "2023-12-01 10:00:00"],
    "Resolution code": ["", "Fixed", "", "", "Fixed", "Escalated", "", "", "", "Fixed"]
}

# Load data into a DataFrame
df = pd.DataFrame(data)

# Convert dates to datetime objects for analysis
df['Opened'] = pd.to_datetime(df['Opened'])
df['Updated'] = pd.to_datetime(df['Updated'])
df['Resolved'] = pd.to_datetime(df['Resolved'], errors='coerce')  # Handle NaT values for unresolved tickets

# **Ticket Distribution by Priority**
priority_count = df['Priority'].value_counts()

# **Ticket Status Breakdown**
status_count = df['State'].value_counts()

# **Issue Category Breakdown**
category_count = df['Issue Category'].value_counts()

# **Assignment Group Workload**
assignment_group_count = df['Assignment group'].value_counts()

# **Resolution Code Breakdown**
resolution_count = df['Resolution code'].value_counts()

# **Resolution Time Calculation (for resolved tickets)**
df['Resolution Time'] = df['Resolved'] - df['Opened']
average_resolution_time = df[df['Resolved'].notnull()]['Resolution Time'].mean()

# Display Findings
print("Ticket Distribution by Priority:\n", priority_count)
print("\nTicket Status Breakdown:\n", status_count)
print("\nIssue Category Breakdown:\n", category_count)
print("\nAssignment Group Workload:\n", assignment_group_count)
print("\nResolution Code Breakdown:\n", resolution_count)
print("\nAverage Resolution Time (Resolved Tickets):", average_resolution_time)

# You can also export this to Excel for further review
df.to_excel("service_now_ticket_data_analysis.xlsx", index=False)