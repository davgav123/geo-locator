
var displayMap = L.map('mapid').setView([55, 15], 4);

L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox/streets-v11',
    tileSize: 512,
    zoomOffset: -1,
    accessToken: 'pk.eyJ1IjoiZGF2Z2F2IiwiYSI6ImNrdTdmYjZqdTU3YXUyb25xaGhhMWlndDUifQ.GXEmjuG3NmH23uvFsOIw-g'
}).addTo(displayMap);

var circleLayer = null;

function displayPoints() {
    var country = document.getElementById("inputCountry").value;
    var filterCondition = document.getElementById("inputCondition").value;

    if (filterCondition == "everything") {
        console.log("select filter!");
        window.alert("Filter not selected!");
        return ;
    }

    httpGet(
        "http://localhost:8080/selection?country=" + country + "&param=" + filterCondition,
        function(err, returnedData) {
            if (err) {
                console.log("error text:\n", err)
                return ;
            }

            if (circleLayer != null) {
                displayMap.removeLayer(circleLayer);
            }

            var data = JSON.parse(returnedData);
            var circles = createCircles(data);
            circleLayer = L.layerGroup(circles);
            circleLayer.addTo(displayMap);

            var xs = data.map(e => e[0]);
            var ys = data.map(e => e[1]);

            viewCoords = [
                xs.reduce((a, c) => a + c) / xs.length,
                ys.reduce((a, c) => a + c) / ys.length
            ];

            max_x = Math.max.apply(null, xs)
            max_y = Math.max.apply(null, ys)
            min_x = Math.min.apply(null, xs)
            min_y = Math.min.apply(null, ys)

            displayMap.setView(viewCoords);
            displayMap.flyToBounds(L.latLngBounds([[max_x, max_y], [min_x, min_y]]))
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

function createCircles(data) {
    return data.map(addCircle);
}

function addCircle(coord) {
    return L.circle(coord, {
        color: 'red',
        fillColor: '#f03',
        fillOpacity: 0.45,
        radius: 10
    });
}
