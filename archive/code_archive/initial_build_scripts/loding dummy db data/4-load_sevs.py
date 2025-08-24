import psycopg2

connection = psycopg2.connect(
   host="localhost",  # Use localhost when port forwarding is active
   port="5432",
   database="fleetconnect_db",
   user="postgres",
   password="mysecretpassword"
)

try:
   cursor = connection.cursor()
   
   security_events = [
    ('001', '001', 'Invalid CAN ID', 'Invalid Can Message ID was sent on the CAN network'),
    ('002', '002', 'CPU Usage', 'Uraeus hosting ECU is facing a High CPU usage that might cause system issue.'),
    ('003', '003', 'Bus Flood Attack', "Very simple denial-of-service attack: transmit CAN frames as fast as possible to soak up bus bandwidth, cause legitimate frames to be delayed and for parts of the system to fail when frames don't turn up on time."),
    ('004', '004', 'IdsM Sev', 'IdsM Security Event.'),
    ('005', '005', 'NRC', 'NRC Security Event.'),
    ('006', '006', 'MAC_ADDR_PERIODICITY_SEV_ID', 'Unexpected MAC address periodicity event detected'),
    ('0006', '0006', 'AI Model Detected Fuzzing', 'Fuzzing Attack Detected'),
    ('0007', '0007', 'AI Model Detected Spoofing', 'Spoofing Attack Detected'),
    ('007', '007', 'Unknown MAC address', 'Unknown IP address event detected'),
    ('008', '008', 'Unknown IP address', 'Unknown MAC address event detected'),
    ('009', '009', 'IP_ADDR_PERIODICITY_SEV_ID', 'Unexpected IP address periodicity event detected'),
    ('010', '010', 'Man in The Middle', 'MiTM attack has been detected detected for CAN message due to Invalid CMAC'),
    ('011', '011', 'Replay Attack', 'Replay Attack has been detected for CAN message due to Invalid sequence number'),
    ('012', '012', 'Spoofing ECU Comm', 'Spoofing ECU Comm has been detected for CAN message'),
    ('32776', '32776', 'Unexpected Periodicity SEv for CAN', 'Attacker sends frames with different frequency.'),
    ('32777', '32777', 'Access Control Monitor Violation SEv', 'Attacker try to access critical files.'),
    ('32778', '32778', 'Falco Monitor SEV', 'Monitor Falco tool that checks for (Privilege escalation using privileged containers, Read/Writes to well-known directories such as /etc, /usr/bin, /usr/sbin, etc, ... )'),
    ('0x800B', '0x800B', 'Fuzzing on CAN', 'A fuzzing attack involves sending malformed, unexpected, or random data to Electronic Control Units (ECUs) via the CAN bus to uncover vulnerabilities.'),
    ('013', '013', 'REPLAY_SEV', 'Attackers capture valid TCP packets and resend them later, disrupting stateful protocols.'),
    ('014', '014', 'MITM_SEV', 'ARP spoofing (a.k.a. ARP poisoning) tricks devices into sending traffic to the attacker by faking IP→MAC mappings.'),
    ('015', '015', 'SPOOF_SEV', 'An attacker forges the source MAC or IP in packets, impersonating another device.')
   ]

   for sev_id, business_id, name, description in security_events:
       # Check if security event exists
       cursor.execute("SELECT id FROM security_event WHERE business_id = %s AND is_deleted = false", (business_id,))
       if cursor.fetchone() is None:
           cursor.execute("""
               INSERT INTO security_event (
                   name,
                   description,
                   status,
                   is_deleted,
                   business_id
               )
               VALUES (%s, %s, 'Active', false, %s);
           """, (name, description, business_id))

   connection.commit()
   print("Successfully inserted security events.")

except Exception as e:
   print(f"Error: {e}")
   connection.rollback()

finally:
   if cursor:
       cursor.close()
   if connection:
       connection.close()