const {CorrelationService} = require('./dist/commonjs/index');

function registerInContainer(container) {
  container
    .register('CorrelationService', CorrelationService)
    .dependencies('CorrelationRepository', 'IamService', 'ProcessDefinitionRepository');
}

module.exports.registerInContainer = registerInContainer;
