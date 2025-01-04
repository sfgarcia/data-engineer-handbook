# Profit

1. Types of data:
a. Revenue from subscriptions
b. Costs from the cloud reported by the Ops team
c. Aggregated salaries by team
d. Profit by active user on the platform

2. Primary owner: Finance team

3. Secondary owner: Data Engineering team

4. Common issues:
a. Cloud costs arrive late from the Ops team. The best solution is to ping the manager on Slack, so the team starts
working on delivering the data as quickly as possible.
b. Salaries are readjusted, but not reflected on the aggregated table. One way to monitor this issue would be to
add an alert if salaries don't change on the months they are scheduled to be readjusted.

5. SLA: Numbers will be reviewed on a weekly basis by the management team

6. Oncall schedule: Monitored by BI on Finance team and oncall rotates on a weekly basis.
There are 8 team members on BI, so every member would get 1 oncall week every 2 months.
Holidays are splitted evenly on team members

# Growth

1. Types of data:
a. Number of monthly active users
b. Number of monthly churned users
c. Number of users that registered, but where never active

2. Primary owner: Growth team

3. Secondary owner: Data Engineering team

4. Common issues:
a. Data from some products arrives late, so the metrics are delayed. Best solution is to ping the corresponding Product Manager,
so he can prioritize the solution on the team's backlog.

5. SLA: Data should land everyday 4 hours after midnight

6. Oncall schedule: Monitored by Data Scientist on the Growth team and oncall rotates on a monthly basis.
There are 4 Data Scientist on Growth, so every member would get 1 oncall week every 1 month.
Holidays are splitted evenly on team members