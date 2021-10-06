
var displayMap = L.map('mapid').setView([45, 15], 5);

L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox/streets-v11',
    tileSize: 512,
    zoomOffset: -1,
    accessToken: 'pk.eyJ1IjoiZGF2Z2F2IiwiYSI6ImNrdTdmYjZqdTU3YXUyb25xaGhhMWlndDUifQ.GXEmjuG3NmH23uvFsOIw-g'
}).addTo(displayMap);

addCircle([44.82, 20.45])

function addCircle(coord) {
    L.circle(coord, {
        color: 'red',
        fillColor: '#f03',
        fillOpacity: 0.6,
        radius: 500
    }).addTo(displayMap)
}