# General command excecution
```mermaid
sequenceDiagram    
    participant User
    participant PartM as Partition <br> Manager
    participant GUI
    participant PO as Process <br> Orchestrator
    participant TC as Top <br> Controller
    participant A as Apps

    User ->> PartM: Boot
    PartM ->> GUI: Start new GUI
    Note over GUI: c'tor command sender 
    Note over GUI: c'tor status listener
    GUI ->> GUI: Listen for notifications
    GUI ->> PartM: Notify c'tor complete

    PartM ->> PO: Boot session
    Note over PO: c'tor status notifier
    PO ->> GUI: Handshake
    PO ->> PartM: Handshake
    Note over PO: c'tor command receiver
    PO ->> PO: Listen for notifications
    PO ->> TC: Boot top controller
    Note over TC: c'tor status notifier
    Note over TC: c'tor command receiver
    TC ->> TC: Wait for commands
    Note over TC: c'tor FSM 
    Note over TC: c'tor status listener 
    TC ->> TC: Listen for notifications
    TC ->> A: Boot applications and controllers
    Note over A: c'tor
    A ->> TC: Handshake
    A ->> TC: Notification: boot complete
    Note over TC: c'tor command sender
    TC ->> PO: Notification: boot complete
    PO ->> PartM: Notification: boot complete

    PartM ->> User: Redirect User to GUI
```

# List available commands
```mermaid
sequenceDiagram
    participant User
    participant C as Controller
    participant A1 as Authenticator
    participant A2 as Authorizer

    User ->> C: Command request list
    C ->> A1: Check user <br> authentication <br> with token
    A1 ->> C: Authentication <br> result
    C ->> A2: Check user <br> authorized actions <br> with username
    A2 ->> C: Return user <br> authorized actions
    C ->> User: Command list

```