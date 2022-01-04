package ai.salesken.cdr.importer.transformer;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import ai.salesken.cdr.importer.constants.AircallConstants;
import ai.salesken.cdr.importer.constants.ClassType;
import ai.salesken.cdr.importer.constants.ResponseCodes;
import ai.salesken.cdr.importer.constants.ResponseMessages;
import ai.salesken.cdr.importer.enums.CdrProviderType;
import ai.salesken.cdr.importer.exception.BadRequestException;
import ai.salesken.cdr.importer.exception.JsonMappingException;
import ai.salesken.model.cdr.importer.SaleskenCallObject;
import ai.salesken.model.cdr.importer.elk.log.CallEventLog;
import ai.salesken.model.cdr.importer.elk.log.CallEventLogDao;
import ai.salesken.model.cdr.importer.elk.log.CdrImportEventNames;
import ai.salesken.utils.AircallUtlity;

/**
 * This class will be responsible for transforming CDR Objects provided from CDR
 * to DTO for service usage
 *
 * @author Jayant
 *
 */

public class CDRToDTO {

	/**
	 * For logging our work activity on ELK
	 */
	CallEventLog callEventLog = new CallEventLog();

	/**
	 * Will help to add logs for getting current workflow information via logs
	 */
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Object mapper will be used to map json String to a java object
	 */
	ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	Gson gson = new Gson();

	/**
	 * Will convert CDR Object to SaleskenCDRDto
	 *
	 * @param aircallCDR -- Object provided by Aircall
	 * @return -- will return an DTO useful for Aircall Service
	 * @throws NotImplementedException                -- functionality
	 *                                                NotImplemented yet
	 *
	 * 
	 * @throws JsonsaleskenAircallCDRmappingException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * 
	 * 
	 * 
	 * 
	 * @author Jayant
	 * @throws BadRequestException -- represents invalid request
	 */

	/**
	 * 
	 * Sample Salesken CallObject
	 * 
	 * <code> [providerName=Aircall, providerCallId=593530860, direction=inbound,
	 * callerId=+1 844-984-3847, leg1Number=+1 844-984-3847, leg2Number=+1
	 * 585-449-0382, disconnectedBy=null, callType=null, recordingUrl=null,
	 * disposition=null, duration=13, callStatus=null, providerCallStatus=done,
	 * ringTime=1631900884, callRating=null, talkRatio=null, agentIdentifier=null,
	 * dealId=null, owner=null, stageId=null, taskId=null, companyId=null,
	 * rawJson={JSON String will be here}, cdrId=593530860, startedAt=null,
	 * endedAt=2021-09-17 23:18:17.0, createdAt=2021-09-17 23:18:17.0,
	 * updatedAt=null, providerDetails=null] </code>
	 */

	public ArrayList<SaleskenCallObject> convertAircallCDRToAircallDto(String uuid, Integer companyId,
			JSONObject aircallCDR, HashMap<String, String> saleskenAircallCDRmapping)
			throws JsonMappingException, IllegalArgumentException, IllegalAccessException, BadRequestException {

		logger.info("Conversion from JsonObject to AircallDto Started ... ");

		if (aircallCDR == null) {

			logger.error("Aircall cdr json object sent is null aircallCDR = " + aircallCDR);
			throw new BadRequestException(ResponseMessages.AIRCALL_CDR_JSON_NULL, ResponseCodes.BAD_REQUEST);

		} else if (saleskenAircallCDRmapping.size() <= 0) {

			logger.error("Aircall provider setting is null saleskenAircallCDRmapping = " + saleskenAircallCDRmapping);

			throw new BadRequestException(ResponseMessages.AIRCALL_PROVIDER_SETTING_NULL, ResponseCodes.BAD_REQUEST);
		}

		ArrayList<SaleskenCallObject> saleskenCDRDtoList = new ArrayList<>();

		// Fetching fields of SaleskencallObject
		Class<?> saleskencallObj = SaleskenCallObject.class;

		// Fetching all fields of Salesken
		Field[] saleskenCallObjectFields = saleskencallObj.getDeclaredFields();

		try {

			JSONArray callDetailsList = aircallCDR.getJSONArray("calls");

			logger.info("CallDetailsList as JSON Array fetched from aircallCDR object = " + callDetailsList.toString());

			// creating a list of SaleskenCDRDto objects
			for (int i = 0; i < callDetailsList.length(); i++) {
				SaleskenCallObject callCDRDto = new SaleskenCallObject();
				// converted to JOSNOBject
				JSONObject callObject = callDetailsList.getJSONObject(i);
				for (int j = 0; j < saleskenCallObjectFields.length; j++) {
					logger.info(saleskenCallObjectFields[j].getName() + " -- "
							+ saleskenAircallCDRmapping.get(saleskenCallObjectFields[j].getName()));

					// setting private fields accessible
					saleskenCallObjectFields[j].setAccessible(true);

					if (saleskenAircallCDRmapping.get(saleskenCallObjectFields[j].getName()) != null) {
						// if data is hierarchial then we need to use another method to set data into
						// our field
						if (saleskenAircallCDRmapping.get(saleskenCallObjectFields[j].getName())
								.split("\\.").length <= 1) {
							// if data is not hierarchial then lets move forward in an easy way
							if (!saleskenCallObjectFields[j].getName().equals(AircallConstants.PROVIDER_NAME)) {
								// if callObject have required data
								if (callObject.has(saleskenAircallCDRmapping.get(saleskenCallObjectFields[j].getName()))
										|| !(callObject.isNull(saleskenAircallCDRmapping
												.get(saleskenCallObjectFields[j].getName())))) {
									// check for all kinds of dates
									if (!saleskenCallObjectFields[j].getType().getName()
											.equalsIgnoreCase(ClassType.TIMESTAMP)
											&& !saleskenCallObjectFields[j].getName()
													.equalsIgnoreCase(AircallConstants.RING_TIME)) {

										setValue(saleskenCallObjectFields[j], callCDRDto,
												callObject
														.get(saleskenAircallCDRmapping
																.get(saleskenCallObjectFields[j].getName()))
														.toString());
									} else {
										// Aircall can send value of dates as a String "null" so checking for that too
										if (!callObject
												.get(saleskenAircallCDRmapping
														.get(saleskenCallObjectFields[j].getName()))
												.toString().equalsIgnoreCase(AircallConstants.NULL_STRING)) {
											// if variable is for ringTime then we need to calculate it
											if (!saleskenCallObjectFields[j].getName()
													.equalsIgnoreCase(AircallConstants.RING_TIME)) {
												saleskenCallObjectFields[j].set(callCDRDto,
														timeFormatter(callObject
																.get(saleskenAircallCDRmapping
																		.get(saleskenCallObjectFields[j].getName()))
																.toString()));
											} else {
												// called in case of RingTime
												saleskenCallObjectFields[j].set(callCDRDto, getRingTime(callObject));
											}
										} else {
											saleskenCallObjectFields[j].set(callCDRDto, null);
										}
									}
								} else {
									// if callObject doesn't have required data then set value as null
									saleskenCallObjectFields[j].set(callCDRDto, null);
								}
							} else {
								saleskenCallObjectFields[j].set(callCDRDto,
										saleskenAircallCDRmapping.get(saleskenCallObjectFields[j].getName()));
							}
							// if hierarchial data is present
						} else {

							String[] nodes = saleskenAircallCDRmapping.get(saleskenCallObjectFields[j].getName())
									.split("\\.");

							if (!callObject.isNull(nodes[0]) && callObject.has(nodes[0])) {
								JSONObject childNode = callObject.getJSONObject(nodes[0]);

								// finding value of last node
								for (int k = 1; k < nodes.length; k++) {
									if (childNode != null)
										if (k != nodes.length - 1)
											childNode = childNode.getJSONObject(nodes[k]);
										else {
											// check for all kinds of dates
											if (!saleskenCallObjectFields[j].getType().getName()
													.equalsIgnoreCase(ClassType.TIMESTAMP)) {
												// if childNode is null
												if (childNode.has(nodes[k]) && !childNode.isNull(nodes[k]))
													setValue(saleskenCallObjectFields[j], callCDRDto,
															childNode.get(nodes[k]).toString());
												else
													saleskenCallObjectFields[j].set(callCDRDto, null);

											} else {
												// Aircall can send value of dates as a String "null" so checking for
												// that too
												if (!childNode.get(nodes[k]).toString().toString()
														.equalsIgnoreCase("null")) {
													saleskenCallObjectFields[j].set(callCDRDto,
															timeFormatter(childNode.get(nodes[k]).toString()));
												} else {
													saleskenCallObjectFields[j].set(callCDRDto, null);
												}
											}
										}
								}
							} else {
								saleskenCallObjectFields[j].set(callCDRDto, null);
							}
						}
					} else {
						saleskenCallObjectFields[j].set(callCDRDto, null);
					}
				}

				// set Raw json Object
				callCDRDto.setRawJson(callObject.toString());
				// setting companyId associated with dialer
				callCDRDto.setCompanyId(companyId);
				logger.info("CDR Object  " + i + 1 + " prepared = " + callCDRDto);

				saleskenCDRDtoList.add(callCDRDto);
			}

		} catch (JsonSyntaxException e) {

			// sending data to elk in case of transformation failure
			CallEventLogDao.getInstance()

					.save(callEventLog.getCallEventLogInstance(uuid, CdrImportEventNames.cdrtransform.name(), companyId,
							-1, AircallUtlity.getElkString(aircallCDR.getJSONArray("calls").toString()), false,
							AircallUtlity.getElkString(saleskenCDRDtoList.toString()), e.getMessage(),
							ResponseCodes.BAD_REQUEST, CdrProviderType.AirCall.name()));

			// Elk needs to be called here
			logger.error("Error hit while saleskenAircallCDRmapping  = " + e.getMessage());
			throw new JsonMappingException(ResponseMessages.JSON_MAPPING_EXCEPTION,
					ResponseCodes.INTERNAL_SERVER_ERROR);
		} catch (JSONException e) {

			// sending data to elk in case of transformation failure
			CallEventLogDao.getInstance()

					.save(callEventLog.getCallEventLogInstance(uuid, CdrImportEventNames.cdrtransform.name(), companyId,

							-1, AircallUtlity.getElkString(aircallCDR.getJSONArray("calls").toString()), false,
							AircallUtlity.getElkString(saleskenCDRDtoList.toString()), e.getMessage(),
							ResponseCodes.BAD_REQUEST, CdrProviderType.AirCall.name()));

			logger.error("Error hit while saleskenAircallCDRmapping = " + e.getMessage());
			// Elk needs to be called here
			throw new JsonMappingException(ResponseMessages.JSON_MAPPING_EXCEPTION,
					ResponseCodes.INTERNAL_SERVER_ERROR);
		}

		CallEventLogDao.getInstance()
				.save(callEventLog.getCallEventLogInstance(uuid, CdrImportEventNames.cdrtransform.name(), companyId, -1,
						AircallUtlity.getElkString(aircallCDR.getJSONArray("calls").toString()), true,
						AircallUtlity.getElkString(saleskenCDRDtoList.toString()), "", "",
						CdrProviderType.AirCall.name()));

		return saleskenCDRDtoList;

	}

	/**
	 * Will be used to fetch ringTime
	 * 
	 * @param callObject
	 * @return
	 * 
	 * @author Jayant
	 */
	public Integer getRingTime(JSONObject callObject) {

		logger.info("Inside getRingTime with JsonObject = " + callObject.toString());
		// if any of the value is null then return 0
		if (!callObject.isNull(AircallConstants.ANSWERED_AT_KEY) && !callObject.isNull(AircallConstants.STARTED_AT_KEY)
				&& callObject.has(AircallConstants.ANSWERED_AT_KEY)
				&& callObject.has(AircallConstants.STARTED_AT_KEY)) {

			Integer ringTime = Integer.parseInt(
					String.valueOf((Long.parseLong(callObject.get(AircallConstants.ANSWERED_AT_KEY).toString())
							- Long.parseLong(callObject.get(AircallConstants.STARTED_AT_KEY).toString()))));

			return ringTime;

		} else {
			return 0;
		}
	}

	/**
	 * Will be used to convert UnixTime to Java Timestamp
	 *
	 * @param unixTime -- time provided by dialer
	 * @return -- Timestamp
	 */
	public Timestamp timeFormatter(String unixTime) {

		logger.info("inside timeFormatter with unixTime = " + unixTime);
		if (unixTime != null) {
			// the format of date
			SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSS");
			return Timestamp.valueOf(sdf.format(new java.util.Date(Long.parseLong(unixTime) * 1000L)));

		} else {
			logger.error("Null UnixTime passed , unixTime= " + unixTime);
			return null;
		}
	}

	/**
	 * 
	 * @param field -- contains class field
	 * @param obj   -- represents current SaleskenCallObect
	 * @param value -- value that needs to be set
	 * @return -- true or false as result
	 * 
	 * @throws IllegalArgumentException -- Represents IllegalArgumentException if
	 *                                  raised
	 * @throws IllegalAccessException   -- represents IllegalAccessException if try
	 *                                  to access private fields
	 * 
	 * @author Jayant
	 * @throws JsonMappingException -- thrown with mapping Exception
	 * 
	 */
	public boolean setValue(Field field, SaleskenCallObject obj, String value)
			throws IllegalArgumentException, IllegalAccessException, JsonMappingException {

		logger.info("Inside setValue ...");
		logger.debug("Inside setValue Filed name =  " + field.getName() + " -- value = " + value + " -- type = "
				+ field.getType());

		boolean result = true;

		try {

			Class<?> type = field.getType();

			switch (type.getName()) {

			case ClassType.STRING:
				field.set(obj, value);
				break;

			case ClassType.INTEGER:
				field.set(obj, Integer.parseInt(value));
				break;

			case ClassType.FLOAT:
				field.set(obj, Float.parseFloat(value));
				break;

			case ClassType.DOUBLE:
				field.set(obj, Double.parseDouble(value));
				break;

			default:
				logger.error("No dataType matched null will be entered");
				field.set(obj, null);
				result = false;
				break;
			}
		} catch (Exception e) {
			logger.error("Error caused while setting value for specific data type = " + e.getMessage());
			throw new JsonMappingException(ResponseMessages.JSON_MAPPING_EXCEPTION,
					ResponseCodes.INTERNAL_SERVER_ERROR);
		}
		return result;

	}

}