document.addEventListener("DOMContentLoaded", () => {
    console.log("JavaScript loaded successfully!");

    // Fetch live vehicle data every 5 seconds
    const fetchVehicles = () => {
        fetch("/api/vehicles")
            .then(response => response.json())
            .then(data => {
                console.log("Live vehicles:", data.vehicles);
                // TODO: update your map here using data.vehicles
            })
            .catch(error => console.error("Error fetching vehicles:", error));
    };

    // Fetch alerts
    const fetchAlerts = () => {
        fetch("/api/alerts")
            .then(response => response.json())
            .then(data => {
                console.log("Service alerts:", data);
                // TODO: display alerts in your UI
            })
            .catch(error => console.error("Error fetching alerts:", error));
    };

    // Initial fetch and periodic refresh
    fetchVehicles();
    fetchAlerts();
    setInterval(fetchVehicles, 5000);
});
