var status = require("../utils/statusCodes.js");
var messages = require("../utils/statusMessages.js");
var express = require('express');
var router = express.Router();
var SwipeReaderModel = require('../models/swipeReader');
var KyrosModel = require('../models/kyros');
var net = require('net');
var moment = require('moment');

// Gestor para el envio de eventos
var EventManager = require("../bl/eventManager.js")

// Fichero de propiedades
var PropertiesReader = require('properties-reader');
var properties = PropertiesReader('./api.properties');

// Definición del log
var fs = require('fs');
var log = require('tracer').console({
    transport : function(data) {
        //console.log(data.output);
        fs.open(properties.get('main.log.file'), 'a', 0666, function(e, id) {
            fs.write(id, data.output+"\n", null, 'utf8', function() {
                fs.close(id, function() {
                });
            });
        });
    }
});

/**
* @apiDefine LoginError
*
* @apiError UserNotFound The id of the User was not found
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -5,
*        "description": "Invalid user or password"
*      }
*     }
*/

/**
* @apiDefine PermissionError
*
* @apiError NotAllow Access not allow to User
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -4,
*        "description": "User not authorized"
*      }
*     }
*/

/** @apiDefine TokenError
*
* @apiError TokenInvalid The token is invalid
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -5,
*        "description": "Invalid token"
*      }
*     }
*/

/** @apiDefine TokenExpiredError
*
* @apiError TokenExpired The token is expired
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -5,
*        "description": "Token expired"
*      }
*     }
*/

/** @apiDefine MissingParameterError
*
* @apiError MissingParameter Missing parameter
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -4,
*        "description": "Missing parameter"
*      }
*     }
*/

/** @apiDefine MissingRegisterError
*
* @apiError MissingRegister Missing register
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -1000,
*        "description": "Missing element"
*      }
*     }
*/

/** @apiDefine IdNumericError
*
* @apiError IdNumeric Id numeric error
*
* @apiErrorExample Error-Response:
*     https/1.1 202
*     {
*     "response": {
*        "status": -9,
*        "description": "The id must be numeric"
*      }
*     }
*/

/** @apiDefine TokenHeader
*
* @apiHeader {String} x-access-token JSON Web Token (JWT)
*
* @apiHeaderExample {json} Header-Example:
*     {
*       "x-access-token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE0MzIyMTg2ODc1ODksImlzcyI6InN1bW8iLCJyb2xlIjoiYWRtaW5pc3RyYXRvciJ9._tYZLkBrESt9FwOccyvripIsZR5S0m8PLZmEgIDEFaY"
*     }
*/

  /* POST. Add New Personnel’s Swipe */
  /**
   * @api {post} /PersonnelSwipe Add New Personnel’s Swipe
   * @apiName PostAddPersonnelSwipe
   * @apiGroup personnel_swipe
   * @apiVersion 1.0.1
   * @apiDescription Add New Personnel’s Swipe
   * @apiSampleRequest https://sumo.kyroslbs.com/PersonnelSwipe
   *
   * @apiParam {String} timestamp Date in ISO format
   * @apiParam {String} cardIdentifier Personnel identifier
   * @apiParam {String} cardReaderIdentifier Swipe identifier
   * @apiSuccess {String} message Result message
   * @apiSuccessExample Success-Response:
   *     https/1.1 201
   *     {
   *     "response": {
   *        "status": 0,
   *        "description": "PersonnelSwipe created"
   *      }
   *     }
   *
   * @apiUse TokenHeader
   * @apiUse PermissionError
   * @apiUse TokenError
   * @apiUse TokenExpiredError
   */
  router.post('/PersonnelSwipe0/', function(req, res)
  {
    // Enviar trama wonde
    //1013973778,20120519161619,-3.784988,43.398126,96,246,14,9,2,0.0,0.9,3836
    //101,20150612120119,-3.784988,43.398126,96,246,14,9,2,0.0,0.9,3836
    //Device ID,DateTime,Longitude,Latitude,Speed,Heading,Altitude,Satellite,Event ID,Mileage,Hdop,Battery mA


      log.info ("Procesando POST Add New Personnel’s Swipe");

      var timestamp = req.params.timestamp || req.query.timestamp || req.body.timestamp;
      var vehicleLicense = req.params.cardIdentifier || req.query.cardIdentifier || req.body.cardIdentifier;
      var swipeName = req.params.cardReaderIdentifier || req.query.cardReaderIdentifier || req.body.cardReaderIdentifier;

      log.debug("  -> timestamp:            " + timestamp);
      log.debug("  -> cardIdentifier:       " + vehicleLicense);
      log.debug("  -> cardReaderIdentifier: " + swipeName);

      if (timestamp == null || vehicleLicense == null || swipeName == null) {
        //res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.MISSING_PARAMETER}})
        res.status(400).json({"code":400,"message":"Missing parameter"});
      }
      else if (!moment(timestamp,'YYYY-MM-DDTHH:mm:ss.SSSZ', true).isValid()) {
        res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.DATE_INCORRECT}})
        //res.status(400).json({"code":400,"message":"Missing parameter"});
      }
      else
      {
      // Limpiar la fecha
      var fecha = moment(timestamp,'YYYY-MM-DDTHH:mm:ss.SSSZ');
      var pos_date = fecha.format("YYYYMMDDHHmmss");
      var epoch = moment(fecha);
      //log.info("----------->"+epoch);

      // Leer el imei
      KyrosModel.getVehicle(vehicleLicense,function(error, data)
      {
        if (data == null)
        {
          log.debug ("Error el leer el imei");
          res.status(202).json({"response": {"status":status.STATUS_FAILURE,"description":messages.DB_ERROR}})
        }
        else if (data[0] == null)
        {
          log.debug ("El imei no existe");
          res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.PARAMETER_ERROR}})
        }
        else {
          if (typeof data !== 'undefined' && data.length > 0)
          {
            var imei = data[0].imei;

            // Leer las coordenadas del swipe
            SwipeReaderModel.getSwipeByName(swipeName,function(error, data)
            {
              if (data == null)
              {
                log.debug ("Error el leer las coordenadas del swipe");
                res.status(202).json({"response": {"status":status.STATUS_FAILURE,"description":messages.DB_ERROR}})
              }
              else if (data[0] == null)
              {
                log.debug ("El swipe indicado no existe");
                res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.PARAMETER_ERROR}})
              }
              else
              {
                //trama wonde. lo cambio por trama especial q va al parser AIS
                //var trama = imei + "," + pos_date + "," + data[0].longitude + "," + data[0].latitude + ",0,0,0,9,2,0.0,0.9,3836," + data[0].description;
                // TRAMA: %imei,typeObt,latitude,longitude,date,altitude,speed
                var trama = "%" + imei + "," + properties.get('kyros.device.personnelSwipe') + "," + data[0].latitude + "," + data[0].longitude + "," + epoch + ",0,0";

                //log.debug ('Trama a enviar a KCS: %s', trama);
                log.debug ("Trama a enviar a KCS (" + properties.get('kyros.kcs.aisip') + ":" + properties.get('kyros.kcs.aisport') + "): " + trama);

                var client = new net.Socket();
                client.connect(properties.get('kyros.kcs.aisport'), properties.get('kyros.kcs.aisip'));

                // Enviar la trama al KCS
                client.write(trama + '\r\n');
                log.debug('Trama enviada!');

                // Hacer llamadas a los que estes subscritos al evento
                var eventManager = new EventManager();
                var eventData;
                if (data[0].situation == 0) {
                  eventData = {
                      eventType : properties.get('sumo.event.personnel.infrastructure.enter'),
                      resourceId: vehicleLicense,
                      infraestructureId: swipeName,
                      latitude: data[0].latitude,
                      longitude: data[0].longitude
                  };
                }
                else {
                  eventData = {
                      eventType : properties.get('sumo.event.personnel.infrastructure.left'),
                      resourceId: vehicleLicense,
                      infraestructureId: swipeName,
                      latitude: data[0].latitude,
                      longitude: data[0].longitude
                  };
                }
                // Enviar evento
                eventManager.sendEvent(eventData);


                client.on('error', function(error) {
                  // Error handling here
                  log.debug ('ERROR en el Socket! '+ error);
                });

                client.on('close', function() {
                  log.debug ('Connection closed');
                });

                client.on('connect', function() {
                  log.debug ('Connection connected');
                });

                //client.destroy();

                // Respuesta
                res.status(201).json({"response": {"status":status.STATUS_SUCCESS,"description":"PersonnelSwipe created"}})
              }
            });
          }
          else
          {
            log.debug ("Error el leer el imei");
            res.status(202).json({"response": {"status":status.STATUS_FAILURE,"description":messages.DB_ERROR}})
          }
        }
      });



      // TODO. Leer si es un swipe de entrada o salida
      /*
      log.debug('Reenviar evento');
      var eventManager = new EventManager();
      var eventData = {
          eventType : properties.get('sumo.event.personnel.infrastructure.left'),

      };
      eventManager.sendEvent(eventData);
      */

    }

  });


router.post('/PersonnelSwipe/', function(req, res)
{
    // Enviar trama wonde
    //1013973778,20120519161619,-3.784988,43.398126,96,246,14,9,2,0.0,0.9,3836
    //101,20150612120119,-3.784988,43.398126,96,246,14,9,2,0.0,0.9,3836
    //Device ID,DateTime,Longitude,Latitude,Speed,Heading,Altitude,Satellite,Event ID,Mileage,Hdop,Battery mA


      log.info ("/PersonnelSwipe");

      var timestamp = req.params.timestamp || req.query.timestamp || req.body.timestamp;
      var vehicleLicense = req.params.cardIdentifier || req.query.cardIdentifier || req.body.cardIdentifier;
      var swipeName = req.params.cardReaderIdentifier || req.query.cardReaderIdentifier || req.body.cardReaderIdentifier;

      log.debug("  -> timestamp:            " + timestamp);
      log.debug("  -> cardIdentifier:       " + vehicleLicense);
      log.debug("  -> cardReaderIdentifier: " + swipeName);

      if (timestamp == null || vehicleLicense == null || swipeName == null) {
        res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.MISSING_PARAMETER}})
      }
      else if (!moment(timestamp,'YYYY-MM-DDTHH:mm:ss.SSSZ', true).isValid()) {
        res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.DATE_INCORRECT}})
      }
      else
      {

        // Conectar a la cola 
        amqp.connect('amqp://' + properties.get('swipe.queue.server'), function(err, conn) {
        if (conn == undefined){
          console.log ("Error de conexion a la cola");
        }

        else {
          log.debug("Conexion a la cola: OK");

          conn.createChannel(function(err, ch) {
          q = properties.get('swipe.queue.name');

          log.debug("Conexion al canal: OK");


          // Limpiar la fecha
      var fecha = moment(timestamp,'YYYY-MM-DDTHH:mm:ss.SSSZ');
      var fecha_nomo = fecha.format("YYYYMMDDHHmmss");
      

      // Leer el imei
      KyrosModel.getVehicle(vehicleLicense,function(error, data)
      {
        if (data == null)
        {
          log.debug ("Error el leer el imei");
          res.status(202).json({"response": {"status":status.STATUS_FAILURE,"description":messages.DB_ERROR}})
        }
        else if (data[0] == null)
        {
          log.debug ("El imei no existe");
          res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.PARAMETER_ERROR}})
        }
        else {
          if (typeof data !== 'undefined' && data.length > 0)
          {
            var imei = data[0].imei;

            // Leer las coordenadas del swipe
            SwipeReaderModel.getSwipeByName(swipeName,function(error, data)
            {
              if (data == null)
              {
                log.debug ("Error el leer las coordenadas del swipe");
                res.status(202).json({"response": {"status":status.STATUS_FAILURE,"description":messages.DB_ERROR}})
              }
              else if (data[0] == null)
              {
                log.debug ("El swipe indicado no existe");
                res.status(202).json({"response": {"status":status.STATUS_VALIDATION_ERROR,"description":messages.PARAMETER_ERROR}})
              }
              else
              {
                //trama wonde. 
                var trama = imei + "," + fecha_nomo + "," + data[0].longitude + "," + data[0].latitude + ",0," + "0" + ",0,9,2,0.0,0.9,3836";

                log.debug ('Trama Swipe que se envia a la cola: %s', trama);

                // Enviar a la cola
                try {
                    ch.sendToQueue(q, new Buffer(trama), {persistent: true});
                    log.debug(" [x] Sent trama to rabbitMQ");
                    ais_queue.info(trama);
                } catch (err) {
                    log.error("ERROR AL METER EN LA COLA: "+err);
                    if (conn != undefined) {
                      conn.close();
                    }
                }

                // Hacer llamadas a los que estes subscritos al evento
                var eventManager = new EventManager();
                var eventData;
                if (data[0].situation == 0) {
                  eventData = {
                      eventType : properties.get('sumo.event.personnel.infrastructure.enter'),
                      resourceId: vehicleLicense,
                      infraestructureId: swipeName,
                      latitude: data[0].latitude,
                      longitude: data[0].longitude
                  };
                }
                else {
                  eventData = {
                      eventType : properties.get('sumo.event.personnel.infrastructure.left'),
                      resourceId: vehicleLicense,
                      infraestructureId: swipeName,
                      latitude: data[0].latitude,
                      longitude: data[0].longitude
                  };
                }
                // Enviar evento
                eventManager.registerPendingEvent(eventData);


                
                // Respuesta
                res.status(201).json({"response": {"status":status.STATUS_SUCCESS,"description":"PersonnelSwipe created"}})
              }
            });
          }
          else
          {
            log.debug ("Error el leer el imei");
            res.status(202).json({"response": {"status":status.STATUS_FAILURE,"description":messages.DB_ERROR}})
          }
        }
      });


          }, function(err) {
            console.error('Connect failed: %s', err);
          });
          setTimeout(function() { conn.close(); }, 500);

    }

  });



module.exports = router;
