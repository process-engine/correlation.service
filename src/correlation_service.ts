import {IIAMService, IIdentity} from '@essential-projects/iam_contracts';

import {NotFoundError} from '@essential-projects/errors_ts';

import {
  Correlation,
  CorrelationFromRepository,
  CorrelationProcessInstance,
  CorrelationState,
  ICorrelationRepository,
  ICorrelationService,
} from '@process-engine/correlation.contracts';

import {
  IProcessDefinitionRepository,
  ProcessDefinitionFromRepository,
} from '@process-engine/process_model.contracts';

/**
 * Groups ProcessModelHashes by their associated CorrelationId.
 *
 * Only use internally.
 */
type GroupedCorrelations = {
  [correlationId: string]: Array<CorrelationFromRepository>,
};

const superAdminClaim: string = 'can_manage_process_instances';
const canReadProcessModelClaim: string = 'can_read_process_model';
const canDeleteProcessModel: string = 'can_delete_process_model';

export class CorrelationService implements ICorrelationService {

  private readonly correlationRepository: ICorrelationRepository;
  private readonly iamService: IIAMService;
  private readonly processDefinitionRepository: IProcessDefinitionRepository;

  constructor(
    correlationRepository: ICorrelationRepository,
    iamService: IIAMService,
    processDefinitionRepository: IProcessDefinitionRepository,
  ) {

    this.correlationRepository = correlationRepository;
    this.iamService = iamService;
    this.processDefinitionRepository = processDefinitionRepository;
  }

  public async createEntry(
    identity: IIdentity,
    correlationId: string,
    processInstanceId: string,
    processModelId: string,
    processModelHash: string,
    parentProcessInstanceId?: string,
  ): Promise<void> {
    return this
      .correlationRepository
      .createEntry(identity, correlationId, processInstanceId, processModelId, processModelHash, parentProcessInstanceId);
  }

  public async getActive(identity: IIdentity): Promise<Array<Correlation>> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const activeCorrelationsFromRepo: Array<CorrelationFromRepository>
      = await this.correlationRepository.getCorrelationsByState(CorrelationState.running);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository>
      = await this.filterCorrelationsFromRepoByIdentity(identity, activeCorrelationsFromRepo);

    const activeCorrelationsForIdentity: Array<Correlation>
      = await this.mapCorrelationList(filteredCorrelationsFromRepo);

    return activeCorrelationsForIdentity;
  }

  public async getAll(identity: IIdentity): Promise<Array<Correlation>> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationsFromRepo: Array<CorrelationFromRepository> = await this.correlationRepository.getAll();

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository> =
      await this.filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    const correlations: Array<Correlation> = await this.mapCorrelationList(filteredCorrelationsFromRepo);

    return correlations;
  }

  public async getByProcessModelId(identity: IIdentity, processModelId: string): Promise<Array<Correlation>> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationsFromRepo: Array<CorrelationFromRepository> =
      await this.correlationRepository.getByProcessModelId(processModelId);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository> =
      await this.filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    const correlations: Array<Correlation> = await this.mapCorrelationList(filteredCorrelationsFromRepo);

    return correlations;
  }

  public async getByCorrelationId(identity: IIdentity, correlationId: string): Promise<Correlation> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    // NOTE:
    // These will already be ordered by their createdAt value, with the oldest one at the top.
    const correlationsFromRepo: Array<CorrelationFromRepository> =
      await this.correlationRepository.getByCorrelationId(correlationId);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository>
      = await this.filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    // All correlations will have the same ID here, so we can just use the top entry as a base.
    const noFilteredCorrelationsFromRepo: boolean = filteredCorrelationsFromRepo.length === 0;
    if (noFilteredCorrelationsFromRepo) {
      throw new NotFoundError('No such correlations for the user.');
    }

    const correlation: Correlation =
      await this.mapCorrelation(correlationsFromRepo[0].id, correlationsFromRepo);

    return correlation;
  }

  public async getByProcessInstanceId(identity: IIdentity, processInstanceId: string): Promise<Correlation> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationFromRepo: CorrelationFromRepository =
      await this.correlationRepository.getByProcessInstanceId(processInstanceId);

    const correlation: Correlation =
      await this.mapCorrelation(correlationFromRepo.id, [correlationFromRepo]);

    return correlation;
  }

  public async getSubprocessesForProcessInstance(identity: IIdentity, processInstanceId: string): Promise<Correlation> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationsFromRepo: Array<CorrelationFromRepository> =
      await this.correlationRepository.getSubprocessesForProcessInstance(processInstanceId);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository> =
      await this.filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    const noFilteredCorrelations: boolean = filteredCorrelationsFromRepo.length === 0;
    if (noFilteredCorrelations) {
      return undefined;
    }

    const correlation: Correlation =
      await this.mapCorrelation(correlationsFromRepo[0].id, correlationsFromRepo);

    return correlation;
  }

  public async deleteCorrelationByProcessModelId(identity: IIdentity, processModelId: string): Promise<void> {
    await this.iamService.ensureHasClaim(identity, canDeleteProcessModel);
    await this.correlationRepository.deleteCorrelationByProcessModelId(processModelId);
  }

  public async finishProcessInstanceInCorrelation(identity: IIdentity, correlationId: string, processInstanceId: string): Promise<void> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);
    await this.correlationRepository.finishProcessInstanceInCorrelation(correlationId, processInstanceId);
  }

  public async finishProcessInstanceInCorrelationWithError(
    identity: IIdentity,
    correlationId: string,
    processInstanceId: string,
    error: Error,
  ): Promise<void> {
    await this.iamService.ensureHasClaim(identity, canReadProcessModelClaim);
    await this.correlationRepository.finishProcessInstanceInCorrelationWithError(correlationId, processInstanceId, error);
  }

  private async filterCorrelationsFromRepoByIdentity(
    identity: IIdentity,
    correlationsFromRepo: Array<CorrelationFromRepository>,
  ): Promise<Array<CorrelationFromRepository>> {

    const isUserSuperAdmin: any = async(): Promise<boolean> => {
      try {
        await this.iamService.ensureHasClaim(identity, superAdminClaim);

        return true;
      } catch (error) {
        return false;
      }
    };

    const userIsSuperAdmin: boolean = identity.userId !== 'dummy_token' && await isUserSuperAdmin();

    // Super Admins can always see everything.
    if (userIsSuperAdmin) {
      return correlationsFromRepo;
    }

    return correlationsFromRepo.filter((correlationFromRepo: CorrelationFromRepository) => {

      // Correlations that were created with the dummy token are visible to everybody.
      const isDummyToken: boolean = correlationFromRepo.identity.userId === 'dummy_token';
      const userIdsMatch: boolean = identity.userId === correlationFromRepo.identity.userId;

      return isDummyToken || userIdsMatch;
    });
  }

  private async mapCorrelationList(correlationsFromRepo: Array<CorrelationFromRepository>): Promise<Array<Correlation>> {
    const groupedCorrelations: GroupedCorrelations = this.groupCorrelations(correlationsFromRepo);

    const uniqueCorrelationIds: Array<string> = Object.keys(groupedCorrelations);

    const mappedCorrelations: Array<Correlation> = [];

    for (const correlationId of uniqueCorrelationIds) {
      const matchingCorrelationEntries: Array<CorrelationFromRepository> = groupedCorrelations[correlationId];

      const mappedCorrelation: Correlation = await this.mapCorrelation(correlationId, matchingCorrelationEntries);
      mappedCorrelations.push(mappedCorrelation);
    }

    return mappedCorrelations;
  }

  private groupCorrelations(correlations: Array<CorrelationFromRepository>): GroupedCorrelations {

    const groupedCorrelations: GroupedCorrelations = {};

    for (const correlation of correlations) {

      const groupHasNoMatchingEntry: boolean = !groupedCorrelations[correlation.id];

      if (groupHasNoMatchingEntry) {
        groupedCorrelations[correlation.id] = [];
      }

      groupedCorrelations[correlation.id].push(correlation);
    }

    return groupedCorrelations;
  }

  private async mapCorrelation(
    correlationId: string,
    correlationsFromRepo?: Array<CorrelationFromRepository>,
  ): Promise<Correlation> {

    const parsedCorrelation: Correlation = new Correlation();
    parsedCorrelation.id = correlationId;
    parsedCorrelation.createdAt = correlationsFromRepo[0].createdAt;

    if (correlationsFromRepo) {
      parsedCorrelation.processInstances = [];

      for (const correlationFromRepo of correlationsFromRepo) {

        /**
         * As long as there is at least one running ProcessInstance within a correlation,
         * the correlation will always have a running state, no matter how many
         * "finished" instances there might be.
         */
        parsedCorrelation.state = parsedCorrelation.state !== CorrelationState.running
                                    ? correlationFromRepo.state
                                    : CorrelationState.running;

        const correlationEntryHasErrorAttached: boolean =
          correlationFromRepo.error !== null
          && correlationFromRepo.error !== undefined;

        if (correlationEntryHasErrorAttached) {
          parsedCorrelation.state = CorrelationState.error;
          parsedCorrelation.error = correlationFromRepo.error;
        }

        const processDefinition: ProcessDefinitionFromRepository =
          await this.processDefinitionRepository.getByHash(correlationFromRepo.processModelHash);

        const processModel: CorrelationProcessInstance = new CorrelationProcessInstance();
        processModel.processDefinitionName = processDefinition.name;
        processModel.xml = processDefinition.xml;
        processModel.hash = correlationFromRepo.processModelHash;
        processModel.processModelId = correlationFromRepo.processModelId;
        processModel.processInstanceId = correlationFromRepo.processInstanceId;
        processModel.parentProcessInstanceId = correlationFromRepo.parentProcessInstanceId;
        processModel.createdAt = correlationFromRepo.createdAt;
        processModel.state = correlationFromRepo.state;
        processModel.identity = correlationFromRepo.identity;

        if (correlationEntryHasErrorAttached) {
          processModel.error = correlationFromRepo.error;
        }

        parsedCorrelation.processInstances.push(processModel);
      }
    }

    return parsedCorrelation;
  }
}
