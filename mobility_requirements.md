# Mobility requirements

- Personal
    - time at which the house is left [time]
        - reasoning: the more popular the time-frame -> the more likely it is that the user will be stuck in traffic -> the more likely it is that they'll consume more energy
    - quanto traffico incontra quando esce di casa
    - how many times per week do you leave the house? [int]
        - how often do you leave the house for work/university/school? [int]
        - how often do you leave the house for grocery shopping? [int]
        - how often do you leave the house for other reasons? [int]
    - how far away is your workplace/university/school? (in kms) [int]
    - do you use public transports? [dropdown] [yes/no]
        - which public transports do you use? [checkboxes] [car/bicycle/scooter/train]
        - how much do you spend in public transports monthly? [int]
    - do you ever walk to your workplace/university/school? [yes/no]

- Family 
    - how many vehicles have been bought in the last 10 (?) years? [int]
    - how many vehicles does your family currently own? [int]
        - out of these vehicles, how many are eco-friendly? [int]
        - in che classe stanno i veicoli della tua fmaiglia? [checkboxes]
    - how many persons in your family own a license? [int]
    - choose every vehicle your family owns [checkboxes] [car/bicycle/motorbike/truck/van/scooter]
    - choose every *green* vehicle your family owns [checkboxes] [car/bicycle/motorbike/van/scooter]
    - which vehicles does your family (you excluded) use daily? [checkboxes] [car/bicycle/motorbike/truck/van/scooter/train]


- Vehicle
    - what is the main vehicle that you use daily? [multi-choice] [car/bicycle/motorbike/truck/van/scooter/train]
        - if selected vehicle is car or motorbike:
            - in quale classe sta il tuo veicolo?
            - qual è l'anno di immatricolazione?
            - what is your vehicle's displacement? [int]
            - what is your vehicle's power source? [multi-choice] [gasoline/diesel/biodiesel/ethanol/electricity/other]
            - how much fuel do you use each month? [int]
            - how much time do you usually need to find parking your vehicle? (in minutes) [int]
    - how much time do you need to reach your workplace/university/school with the vehicle you selected(in minutes)? [int]
