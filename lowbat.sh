limit=$1

export DBUS_SESSION_BUS_ADDRESS=$(cat /home/kqr/.dbus-address)

acpi -b | awk -F'[,:%]' '{print $2, $3}' | {
        read -r status capacity

        if [ "$status" = Discharging -a "$capacity" -lt $limit ]; then
            logger "Critical battery threshold"
            sudo systemctl suspend
        elif [ "$status" = Discharging -a "$capacity" -lt $(($limit*2)) ]; then
            notify-send 'Very low battery' "Battery level ($capacity%) approaching critical"
        elif [ "$status" = Discharging -a "$capacity" -lt $(($limit*4)) ]; then
            notify-send 'Low battery' "Battery level ($capacity%) low"
        fi
}
