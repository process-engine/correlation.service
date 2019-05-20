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

  private readonly _correlationRepository: ICorrelationRepository;
  private readonly _iamService: IIAMService;
  private readonly _processDefinitionRepository: IProcessDefinitionRepository;

  constructor(correlationRepository: ICorrelationRepository,
              iamService: IIAMService,
              processDefinitionRepository: IProcessDefinitionRepository) {

    this._correlationRepository = correlationRepository;
    this._iamService = iamService;
    this._processDefinitionRepository = processDefinitionRepository;
  }

  public async createEntry(identity: IIdentity,
                           correlationId: string,
                           processInstanceId: string,
                           processModelId: string,
                           processModelHash: string,
                           parentProcessInstanceId?: string): Promise<void> {
    return this
      ._correlationRepository
      .createEntry(identity, correlationId, processInstanceId, processModelId, processModelHash, parentProcessInstanceId);
  }

  public async getActive(identity: IIdentity): Promise<Array<Correlation>> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const activeCorrelationsFromRepo: Array<CorrelationFromRepository>
      = await this._correlationRepository.getCorrelationsByState(CorrelationState.running);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository>
      = await this._filterCorrelationsFromRepoByIdentity(identity, activeCorrelationsFromRepo);

    const activeCorrelationsForIdentity: Array<Correlation>
      = await this._mapCorrelationList(filteredCorrelationsFromRepo);

    return activeCorrelationsForIdentity;
  }

  public async getAll(identity: IIdentity): Promise<Array<Correlation>> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationsFromRepo: Array<CorrelationFromRepository> = await this._correlationRepository.getAll();

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository> =
      await this._filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    const correlations: Array<Correlation> = await this._mapCorrelationList(filteredCorrelationsFromRepo);

    return correlations;
  }

  public async getByProcessModelId(identity: IIdentity, processModelId: string): Promise<Array<Correlation>> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationsFromRepo: Array<CorrelationFromRepository> =
      await this._correlationRepository.getByProcessModelId(processModelId);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository> =
      await this._filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    const correlations: Array<Correlation> = await this._mapCorrelationList(filteredCorrelationsFromRepo);

    return correlations;
  }

  public async getByCorrelationId(identity: IIdentity, correlationId: string): Promise<Correlation> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    // NOTE:
    // These will already be ordered by their createdAt value, with the oldest one at the top.
    const correlationsFromRepo: Array<CorrelationFromRepository> =
      await this._correlationRepository.getByCorrelationId(correlationId);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository>
      = await this._filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    // All correlations will have the same ID here, so we can just use the top entry as a base.
    const noFilteredCorrelationsFromRepo: boolean = filteredCorrelationsFromRepo.length === 0;
    if (noFilteredCorrelationsFromRepo) {
      throw new NotFoundError('No such correlations for the user.');
    }

    const correlation: Correlation =
      await this._mapCorrelation(correlationsFromRepo[0].id, correlationsFromRepo);

    return correlation;
  }

  public async getByProcessInstanceId(identity: IIdentity, processInstanceId: string): Promise<Correlation> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationFromRepo: CorrelationFromRepository =
      await this._correlationRepository.getByProcessInstanceId(processInstanceId);

    const correlation: Correlation =
      await this._mapCorrelation(correlationFromRepo.id, [correlationFromRepo]);

    return correlation;
  }

  public async getSubprocessesForProcessInstance(identity: IIdentity, processInstanceId: string): Promise<Correlation> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);

    const correlationsFromRepo: Array<CorrelationFromRepository> =
      await this._correlationRepository.getSubprocessesForProcessInstance(processInstanceId);

    const filteredCorrelationsFromRepo: Array<CorrelationFromRepository> =
      await this._filterCorrelationsFromRepoByIdentity(identity, correlationsFromRepo);

    const noFilteredCorrelations: boolean = filteredCorrelationsFromRepo.length === 0;
    if (noFilteredCorrelations) {
      return undefined;
    }

    const correlation: Correlation =
      await this._mapCorrelation(correlationsFromRepo[0].id, correlationsFromRepo);

    return correlation;
  }

  public async deleteCorrelationByProcessModelId(identity: IIdentity, processModelId: string): Promise<void> {
    await this._iamService.ensureHasClaim(identity, canDeleteProcessModel);
    await this._correlationRepository.deleteCorrelationByProcessModelId(processModelId);
  }

  public async finishProcessInstanceInCorrelation(identity: IIdentity, correlationId: string, processInstanceId: string): Promise<void> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);
    await this._correlationRepository.finishProcessInstanceInCorrelation(correlationId, processInstanceId);
  }

  public async finishProcessInstanceInCorrelationWithError(
    identity: IIdentity,
    correlationId: string,
    processInstanceId: string,
    error: Error,
  ): Promise<void> {
    await this._iamService.ensureHasClaim(identity, canReadProcessModelClaim);
    await this._correlationRepository.finishProcessInstanceInCorrelationWithError(correlationId, processInstanceId, error);
  }

  private async _filterCorrelationsFromRepoByIdentity(
    identity: IIdentity,
    correlationsFromRepo: Array<CorrelationFromRepository>,
  ): Promise<Array<CorrelationFromRepository>> {

    const isUserSuperAdmin: any = async (): Promise<boolean> => {
      try {
        await this._iamService.ensureHasClaim(identity, superAdminClaim);

        return true;
      } catch (error) {
        return false;
      }
    }

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

  /**
   * Maps a given List of CorrelationFromRepository objects into a List of
   * Runtime Correlation objects.
   *
   * @async
   * @param   correlationsFromRepo The Correlations to map.
   * @returns                      The mapped Correlation.
   */
  private async _mapCorrelationList(correlationsFromRepo: Array<CorrelationFromRepository>): Promise<Array<Correlation>> {
    const groupedCorrelations: GroupedCorrelations = this._groupCorrelations(correlationsFromRepo);

    const uniqueCorrelationIds: Array<string> = Object.keys(groupedCorrelations);

    const mappedCorrelations: Array<Correlation> = [];

    for (const correlationId of uniqueCorrelationIds) {
      const matchingCorrelationEntries: Array<CorrelationFromRepository> = groupedCorrelations[correlationId];

      const mappedCorrelation: Correlation = await this._mapCorrelation(correlationId, matchingCorrelationEntries);
      mappedCorrelations.push(mappedCorrelation);
    }

    return mappedCorrelations;
  }

  /**
   * Takes a list of CorrelationFromRepository objects and groups them by their
   * CorrelationId.
   *
   * @param   correlations The Correlations to group.
   * @returns              The grouped Correlations.
   */
  private _groupCorrelations(correlations: Array<CorrelationFromRepository>): GroupedCorrelations {

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

  /**
   * Maps a given list of CorrelationFromRepository objects into a,
   * Correlation object, using the given CorrelationId as a base.
   *
   * @async
   * @param   correlationId           The ID of the Correlation to map.
   * @param   correlationEntriess     The list of entries to map.
   * @returns                         The mapped Correlation.
   */
  private async _mapCorrelation(correlationId: string,
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
          await this._processDefinitionRepository.getByHash(correlationFromRepo.processModelHash);

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
