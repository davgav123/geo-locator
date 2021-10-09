
var displayMap = L.map('mapid').setView([45, 15], 5);

L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox/streets-v11',
    tileSize: 512,
    zoomOffset: -1,
    accessToken: 'pk.eyJ1IjoiZGF2Z2F2IiwiYSI6ImNrdTdmYjZqdTU3YXUyb25xaGhhMWlndDUifQ.GXEmjuG3NmH23uvFsOIw-g'
}).addTo(displayMap);

// addCircle([44.82, 20.45])

function processData() {
    var country = document.getElementById("inputCountry").value
    var filterCondition = document.getElementById("inputCondition").value

    // TODO: check validity of inputs

    httpGet(
        "http://localhost:8080/selection?country=" + country + "&param=" + filterCondition,
        function(err, returnedData) {
            if (err) {
                console.log("error text:\n", err)
                return ;
            }

            var ret = JSON.parse(returnedData)
            ret.forEach(point => addCircle(point))

            var xs = ret.map(e => e[0])
            var ys = ret.map(e => e[1])
            displayMap.setView([
                xs.reduce((a, c) => a + c) / xs.length,
                ys.reduce((a, c) => a + c) / ys.length
            ], 5);
        }
    )
}

function httpGet(url, callback) {
    const httpReq = new XMLHttpRequest();
    httpReq.onreadystatechange = function() {
        if (httpReq.readyState == 4) {
            if (httpReq.status == 200) {
                const data = httpReq.responseText;
                callback(null, data);
            } else {
                const err = new Error(httpReq.statusText);
                callback(err, null);
            }
        }
    };

    httpReq.open("GET", url, true);
    httpReq.send();
}

function addCircle(coord) {
    L.circle(coord, {
        color: 'red',
        fillColor: '#f03',
        fillOpacity: 0.6,
        radius: 500
    }).addTo(displayMap)
}
