from .flow_base.flow_pipe import FlowOutputFilter, FlowPipe, FlowOutputRenamer, FlowSource, FlowSubPipeline, FlowSplitJoinPipe # ,FlowMapPipe, FlowParallelPipe,
from .flow_base.flow_manager import FlowManager
from .flow_pipes.fasta_pipes import LoadFastaPipe, WriteFastaPipe, SelectRandomFastaSequencesPipe
from .flow_pipes.motif_pipes import SampleMotifsFromPWMPipe, ProcessProvidedMotifPipe, GenerateRandomMotifsPipe, ParsePWMPipe
from .flow_pipes.shuffle_pipes import NaiveShufflePipe, DiPairShufflePipe
from .flow_pipes.injection_pipes import InjectMotifsIntoFastaRecordsPipe
from .flow_pipes.utility_pipes import UnitAmountConverterPipe, CommandExecutorPipe
from .flow_base.flow_builder import build_flow
from .flow_pipes.enrichment_pipes import BatchJobExecutorPipe, MemeCommandGeneratorPipe, JobExecutorPipe, HomerCommandGeneratorPipe#, SlurmJobGeneratorPipe
from .flow_pipes.motif_detection_pipes import MotifLocalAlignmentPipe, PWMComparisonPipe,  StringToOneShotPWMPipe, MemeXmlParserPipe, HomerTextParserPipe, MotifSummaryPipe

__all__ = [
    # Base classes
    "FlowManager", 
    "FlowPipe", 
    "FlowOutputFilter", 
    "FlowOutputRenamer", 
    "FlowSource",
    "FlowSubPipeline",
    #"FlowMapPipe",
    #"FlowParallelPipe",
    "FlowSplitJoinPipe",
    
    # Fasta pipes
    "LoadFastaPipe", 
    "WriteFastaPipe", 
    "SelectRandomFastaSequencesPipe", 
    
    # Motif pipes
    "SampleMotifsFromPWMPipe",
    "ProcessProvidedMotifPipe",
    "GenerateRandomMotifsPipe",
    "ParsePWMPipe",
    
    # Shuffle pipes
    "NaiveShufflePipe",
    "DiPairShufflePipe",
    
    # Injection pipes
    "InjectMotifsIntoFastaRecordsPipe",
    
    # Utility pipes
    "UnitAmountConverterPipe",
    "CommandExecutorPipe",
    
    # Enrichment pipes
    "BatchJobExecutorPipe",
    #"SlurmJobGeneratorPipe",
    "MemeCommandGeneratorPipe",
    "JobExecutorPipe",
    "HomerCommandGeneratorPipe",
    
    # Motif detection pipes
    "MemeXmlParserPipe",
    "HomerTextParserPipe",
    "MotifLocalAlignmentPipe",
    "PWMComparisonPipe",
    "StringToOneShotPWMPipe",
    "MotifSummaryPipe",
    
    # Builder
    "build_flow"
]