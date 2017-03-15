/* This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public License
 as published by the Free Software Foundation, either version 3 of
 the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package org.opentripplanner.updater.siri;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.updater.JsonConfigurable;
import org.opentripplanner.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.org.siri.siri20.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.util.List;

public class STIFSiriLiteETHttpTripUpdateSource implements EstimatedTimetableSource, JsonConfigurable {
    private static final Logger LOG =
            LoggerFactory.getLogger(STIFSiriLiteETHttpTripUpdateSource.class);

    /**
     * True iff the last list with updates represent all updates that are active right now, i.e. all
     * previous updates should be disregarded
     */
    private boolean fullDataset = true;

    /**
     * Feed id that is used to match trip ids in the TripUpdates
     */
    private String feedId;

    private String url;

    private ZonedDateTime lastTimestamp = ZonedDateTime.now().minusMonths(1);

    @Override
    public void configure(Graph graph, JsonNode config) throws Exception {
        String url = config.path("url").asText();
        if (url == null) {
            throw new IllegalArgumentException("Missing mandatory 'url' parameter");
        }
        this.url = url;

        this.feedId = config.path("feedId").asText();

    }

    @Override
    public List getUpdates() {
        long t1 = System.currentTimeMillis();
        try {

            InputStream is = HttpUtils.getData(url, "Accept", "application/json");
            if (is != null) {
                // Decode message
                LOG.info("Fetching ET-data took {} ms", (System.currentTimeMillis()-t1));
                t1 = System.currentTimeMillis();

                Siri siri = parseStifJson(is);
                LOG.info("Unmarshalling ET-data took {} ms", (System.currentTimeMillis()-t1));

                if (siri.getServiceDelivery().getResponseTimestamp().isBefore(lastTimestamp)) {
                    LOG.info("Newer data has already been processed");
                    return null;
                }
                lastTimestamp = siri.getServiceDelivery().getResponseTimestamp();

                //All subsequent requests will return changes since last request
                fullDataset = false;
                return siri.getServiceDelivery().getEstimatedTimetableDeliveries();

            }
        } catch (Exception e) {
            LOG.info("Failed after {} ms", (System.currentTimeMillis()-t1));
            LOG.warn("Failed to parse SIRI Lite ET feed from " + url + ":", e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception{
        STIFSiriLiteETHttpTripUpdateSource s = new STIFSiriLiteETHttpTripUpdateSource();
        s.parseStifJson(new FileInputStream("/Users/Lasse/Downloads/anshar/stif.json"));
    }

    private Siri parseStifJson(InputStream is) {
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonServiceDelivery = jsonParser.parse(new InputStreamReader(is))
                .getAsJsonObject().get("Siri")
                .getAsJsonObject().getAsJsonObject("serviceDelivery");

        JsonArray estimatedTimeTableDeliveries = jsonServiceDelivery.getAsJsonArray("estimatedTimetableDelivery");

        Siri siri = new Siri();
        ServiceDelivery serviceDelivery = new ServiceDelivery();

        serviceDelivery.setResponseTimestamp(getTimestamp(jsonServiceDelivery.getAsJsonPrimitive("responseTimestamp").getAsString()));
        serviceDelivery.setProducerRef(getProducerRef(jsonServiceDelivery));
        EstimatedTimetableDeliveryStructure etDeliveryStructure = new EstimatedTimetableDeliveryStructure();
        EstimatedVersionFrameStructure etVersionFrameStruct = new EstimatedVersionFrameStructure();

        for (int i = 0; i < estimatedTimeTableDeliveries.size(); i++) {
            JsonObject etJsonObj = estimatedTimeTableDeliveries.get(i).getAsJsonObject();

            EstimatedVehicleJourney estimatedVehicleJourney = new EstimatedVehicleJourney();
            if (etJsonObj.get("recordedAtTime") != null) {
                estimatedVehicleJourney.setRecordedAtTime(getTimestamp(etJsonObj.get("recordedAtTime").getAsString()));
            }
            estimatedVehicleJourney.setDatedVehicleJourneyRef(getDatedVehicleJourneyRef(etJsonObj));
            estimatedVehicleJourney.setDestinationRef(getDestinationRef(etJsonObj));
            estimatedVehicleJourney.setDirectionRef(getDirectionRef(etJsonObj));
            estimatedVehicleJourney.setLineRef(getLineRef(etJsonObj));
            estimatedVehicleJourney.setEstimatedCalls(getEstimatedCalls(etJsonObj));

            if (etJsonObj.get("publishedLineName") != null) {
                estimatedVehicleJourney.getPublishedLineNames().add(getNameStructure(etJsonObj.get("publishedLineName")));
            }
            if (etJsonObj.get("destinationName") != null) {
                estimatedVehicleJourney.getDestinationNames().add(getNameStructure(etJsonObj.get("destinationName")));
            }

            etVersionFrameStruct.getEstimatedVehicleJourneies().add(estimatedVehicleJourney);

        }

        etDeliveryStructure.getEstimatedJourneyVersionFrames().add(etVersionFrameStruct);
        serviceDelivery.getEstimatedTimetableDeliveries().add(etDeliveryStructure);
        siri.setServiceDelivery(serviceDelivery);
        return siri;
    }

    private RequestorRef getProducerRef(JsonObject jsonServiceDelivery) {
        RequestorRef obj = new RequestorRef();
        obj.setValue(jsonServiceDelivery.getAsJsonObject().get("producerRef").getAsString());
        return obj;
    }

    private ZonedDateTime getTimestamp(String recordedAtTime) {
        return ZonedDateTime.parse(recordedAtTime);
    }

    private EstimatedVehicleJourney.EstimatedCalls getEstimatedCalls(JsonElement etJsonElement) {
        EstimatedVehicleJourney.EstimatedCalls calls = new EstimatedVehicleJourney.EstimatedCalls();
        JsonArray jsonCalls = etJsonElement.getAsJsonObject().get("estimatedCalls").getAsJsonObject().get("estimatedCall").getAsJsonArray();
        for (int i = 0; i < jsonCalls.size(); i++) {
            calls.getEstimatedCalls().add(getEstimatedCall(jsonCalls.get(i)));
        }
        return calls;
    }

    private EstimatedCall getEstimatedCall(JsonElement jsonElement) {
        EstimatedCall call = new EstimatedCall();
        JsonObject callObj = jsonElement.getAsJsonObject();
        if (callObj.get("aimedArrivalTime") != null) {
            call.setAimedArrivalTime(getTimestamp(callObj.get("aimedArrivalTime").getAsString()));
        }
        if (callObj.get("aimedDepartureTime") != null) {
            call.setAimedDepartureTime(getTimestamp(callObj.get("aimedDepartureTime").getAsString()));
        }
        if (callObj.get("expectedArrivalTime") != null) {
            call.setExpectedArrivalTime(getTimestamp(callObj.get("expectedArrivalTime").getAsString()));
        }
        if (callObj.get("expectedDepartureTime") != null) {
            call.setExpectedDepartureTime(getTimestamp(callObj.get("expectedDepartureTime").getAsString()));
        }
        call.setStopPointRef(getStopPointRef(jsonElement));
        return call;
    }

    private StopPointRef getStopPointRef(JsonElement etJsonElement) {
        StopPointRef obj = new StopPointRef();
        obj.setValue(etJsonElement.getAsJsonObject().get("stopPointRef").getAsJsonObject().get("value").getAsString());
        return obj;
    }

    private LineRef getLineRef(JsonElement etJsonElement) {
        LineRef obj = new LineRef();
        obj.setValue(etJsonElement.getAsJsonObject().get("lineRef").getAsJsonObject().get("value").getAsString());
        return obj;
    }

    private DatedVehicleJourneyRef getDatedVehicleJourneyRef(JsonElement etJsonElement) {
        DatedVehicleJourneyRef obj = new DatedVehicleJourneyRef();
        obj.setValue(etJsonElement.getAsJsonObject().get("datedVehicleJourneyRef").getAsJsonObject().get("value").getAsString());
        return obj;
    }

    private DestinationRef getDestinationRef(JsonElement etJsonElement) {
        DestinationRef obj = new DestinationRef();
        obj.setValue(etJsonElement.getAsJsonObject().get("destinationRef").getAsJsonObject().get("value").getAsString());
        return obj;
    }

    private DirectionRefStructure getDirectionRef(JsonElement etJsonElement) {
        DirectionRefStructure obj = new DirectionRefStructure();
        obj.setValue(etJsonElement.getAsJsonObject().get("directionRef").getAsJsonObject().get("value").getAsString());
        return obj;
    }

    private NaturalLanguageStringStructure getNameStructure(JsonElement etJsonObj) {
        NaturalLanguageStringStructure obj = new NaturalLanguageStringStructure();
        obj.setValue(etJsonObj.getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString());
        return obj;
    }

    @Override
    public boolean getFullDatasetValueOfLastUpdates() {
        return fullDataset;
    }
    
    public String toString() {
        return "STIFSiriLiteETHttpTripUpdateSource(" + url + ")";
    }

    @Override
    public String getFeedId() {
        return this.feedId;
    }
}
