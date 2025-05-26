#!/usr/bin/env python3
"""
Flow pipes for detecting injected motifs in MEME results.
These pipes compare injected motifs to discovered motifs using various metrics.
"""
import os
import re
import xml.etree.ElementTree as ET
import numpy as np
from flowline import FlowPipe

# Constants
NUCLEOTIDES = ['A', 'C', 'G', 'T']
DEGENERATE_MAP = {
    'A': ['A'],
    'C': ['C'], 
    'G': ['G'], 
    'T': ['T'],
    'R': ['A', 'G'],       # Purine
    'Y': ['C', 'T'],       # Pyrimidine
    'M': ['A', 'C'],       # Amino
    'K': ['G', 'T'],       # Keto
    'S': ['C', 'G'],       # Strong
    'W': ['A', 'T'],       # Weak
    'B': ['C', 'G', 'T'],  # Not A
    'D': ['A', 'G', 'T'],  # Not C
    'H': ['A', 'C', 'T'],  # Not G
    'V': ['A', 'C', 'G'],  # Not T
    'N': ['A', 'C', 'G', 'T']  # Any
}

class MemeXmlParserPipe(FlowPipe):
    """
    Pipe for parsing MEME XML results files to extract detailed motif information.
    
    Input:
        - output_dir: Directory containing MEME output (with meme.xml)
        - status: Job execution status (must be "COMPLETED" to proceed)
        
    Output:
        - motifs: List of motif objects with detailed information:
            {
                'id': motif ID (e.g. 'motif_1'),
                'name': motif name,
                'consensus': consensus string,
                'width': width of the motif,
                'p_value': p-value of the motif,
                'sites': number of sites,
                'pwm': position weight matrix as a dict with keys for A, C, G, T
            }
    """
    def __init__(self):
        """Initialize the MEME XML parser pipe."""
        inputs = ["output_dir", "status"]
        outputs = ["motifs"]
        super().__init__(inputs=inputs, outputs=outputs, action=self._parse_meme_xml)
    
    def _parse_meme_xml(self, data):
        """Parse MEME XML results to extract detailed motif information."""
        result = {}
        
        # Get required inputs
        output_dir = data.get('output_dir')
        status = data.get('status')
        
        # Validate required inputs
        if not output_dir:
            raise ValueError("Missing required input: output_dir")
        if not status:
            raise ValueError("Missing required input: status")
            
        # Check if the job execution was successful
        if status != "COMPLETED":
            raise ValueError(f"Cannot parse MEME results: job status is {status}, expected COMPLETED")
        
        # Check if the output directory exists
        if not os.path.isdir(output_dir):
            print(f"Output directory does not exist: {output_dir}")
            raise ValueError(f"Output directory does not exist: {output_dir}")
        
        # Check for the meme.xml file, which contains the results
        meme_xml_path = os.path.join(output_dir, "meme.xml")
        if not os.path.isfile(meme_xml_path):
            print(f"MEME XML file not found: {meme_xml_path}")
            raise ValueError(f"MEME XML file not found: {meme_xml_path}")
        
        # Parse the XML file
        try:
            tree = ET.parse(meme_xml_path)
            root = tree.getroot()
            
            # List to store parsed motifs
            motifs = []
            
            # Find all motif elements in the XML
            for motif_element in root.findall('.//motif'):
                motif = {}
                
                # Extract basic motif information
                motif['id'] = motif_element.get('id')
                motif['name'] = motif_element.get('name')
                motif['width'] = int(motif_element.get('width', '0'))
                motif['p_value'] = motif_element.get('p_value')
                motif['sites'] = int(motif_element.get('sites', '0'))
                
                # Extract regular expression (consensus)
                regex_element = motif_element.find('.//regular_expression')
                if regex_element is not None and regex_element.text:
                    motif['consensus'] = regex_element.text.strip()
                else:
                    motif['consensus'] = ""
                
                # Extract PWM from probabilities
                probabilities_element = motif_element.find('.//probabilities/alphabet_matrix')
                if probabilities_element is not None:
                    pwm = {'A': [], 'C': [], 'G': [], 'T': []}
                    
                    for array_element in probabilities_element.findall('./alphabet_array'):
                        for value_element in array_element.findall('./value'):
                            letter_id = value_element.get('letter_id')
                            if letter_id and isinstance(letter_id, str) and letter_id in pwm:
                                pwm[letter_id].append(float(value_element.text or '0.0'))
                                
                    motif['pwm'] = pwm
                else:
                    motif['pwm'] = {'A': [], 'C': [], 'G': [], 'T': []}
                
                # Extract sites/locations
                # sites_element = motif_element.find('.//contributing_sites')
                # if sites_element is not None:
                #     sites = []
                    
                #     for site_element in sites_element.findall('./contributing_site'):
                #         site = {
                #             'sequence_id': site_element.get('sequence_id'),
                #             'position': int(site_element.get('position')),
                #             'strand': site_element.get('strand'),
                #             'p_value': site_element.get('pvalue')
                #         }
                #         sites.append(site)
                        
                #     motif['locations'] = sites
                # else:
                #     motif['locations'] = []
                
                # Add motif to list
                motifs.append(motif)
            
            # Add motifs to result
            result['motifs'] = motifs
            
        except Exception as e:
            print(f"Error parsing MEME XML: {str(e)}")
            raise ValueError(f"Error parsing MEME XML: {str(e)}")
        
        # Return results
        return result

class StringToOneShotPWMPipe(FlowPipe):
    """
    Pipe for converting a nucleotide string to a one-shot PWM representation.
    
    Input:
        - motif_string: The motif string to convert (e.g., "ACCGTG")
        
    Output:
        - pwm: Position weight matrix as dict with keys A, C, G, T and values as lists of probabilities
    """
    def __init__(self):
        """Initialize the string to PWM converter pipe."""
        super().__init__()
    
    def execute(self, data):
        """Convert a motif string to a one-shot PWM."""
        result = {}
        
        # Get required inputs
        motif_string = data.get('motif_string')
        
        # Validate required inputs
        if not motif_string:
            raise ValueError("Missing required input: motif_string")
        
        # Initialize the PWM dictionary
        pwm = {'A': [], 'C': [], 'G': [], 'T': []}
        
        # For each position in the motif string, create a one-shot encoding
        for nucleotide in motif_string.upper():
            if nucleotide == 'A':
                pwm['A'].append(1.0)
                pwm['C'].append(0.0)
                pwm['G'].append(0.0)
                pwm['T'].append(0.0)
            elif nucleotide == 'C':
                pwm['A'].append(0.0)
                pwm['C'].append(1.0)
                pwm['G'].append(0.0)
                pwm['T'].append(0.0)
            elif nucleotide == 'G':
                pwm['A'].append(0.0)
                pwm['C'].append(0.0)
                pwm['G'].append(1.0)
                pwm['T'].append(0.0)
            elif nucleotide == 'T':
                pwm['A'].append(0.0)
                pwm['C'].append(0.0)
                pwm['G'].append(0.0)
                pwm['T'].append(1.0)
            elif nucleotide in DEGENERATE_MAP:
                # Handle degenerate nucleotide codes
                bases = DEGENERATE_MAP[nucleotide]
                prob = 1.0 / len(bases)
                
                pwm['A'].append(prob if 'A' in bases else 0.0)
                pwm['C'].append(prob if 'C' in bases else 0.0)
                pwm['G'].append(prob if 'G' in bases else 0.0)
                pwm['T'].append(prob if 'T' in bases else 0.0)
            else:
                raise ValueError(f"Invalid nucleotide in motif string: {nucleotide}")
        
        # Return the PWM
        result['pwm'] = pwm
        
        return result

class MotifLocalAlignmentPipe(FlowPipe):
    """
    Pipe for performing modified Smith-Waterman local alignment between an injected motif and discovered motifs.
    
    Input:
        - discovered_motifs: List of motif objects with consensus strings (from MemeXmlParserPipe)
        
    Output:
        - matched_motifs: List of motifs with added 'is_match', 'similarity_score', and 'alignment' properties
    """
    def __init__(self, injected_motif, match_score=2, mismatch_score=-1, gap_open=-4, gap_extend=-2, similarity_threshold=0.7):
        """Initialize the motif local alignment pipe."""
        super().__init__(inputs=["discovered_motifs"], outputs=["matched_motifs"])
        self.injected_motif = injected_motif
        self.match_score = match_score
        self.mismatch_score = mismatch_score
        self.gap_open = gap_open
        self.gap_extend = gap_extend
        self.similarity_threshold = similarity_threshold
    
    def parse_consensus_to_sets(self, consensus):
        """Parse a consensus string into a list of sets of allowed nucleotides at each position."""
        allowed_sets = []
        i = 0
        while i < len(consensus):
            if consensus[i] == '[':
                closing_idx = consensus.find(']', i)
                if closing_idx == -1:
                    raise ValueError(f"Invalid consensus string: unclosed bracket at position {i}")
                group = consensus[i+1:closing_idx]
                allowed_sets.append(set(group))
                i = closing_idx + 1
            else:
                if consensus[i] in DEGENERATE_MAP:
                    allowed_sets.append(set(DEGENERATE_MAP[consensus[i]]))
                else:
                    allowed_sets.append({consensus[i]})
                i += 1
        return allowed_sets
    
    def calculate_match_score(self, nuc1, allowed_set):
        """Calculate the score for matching a nucleotide against an allowed set."""
        if nuc1 in allowed_set:
            return self.match_score
        return self.mismatch_score
    
    def smith_waterman(self, seq1, seq2_sets):
        """
        Perform Smith-Waterman local alignment between a sequence and a list of sets of allowed nucleotides.
        
        Returns:
            Tuple of (max_score, seq1_aligned, seq2_aligned, similarity, aligned_length, identity, coverage)
        """
        rows = len(seq1) + 1
        cols = len(seq2_sets) + 1
        score_matrix = np.zeros((rows, cols), dtype=float)
        traceback = np.zeros((rows, cols), dtype=int)  # 0=end, 1=diagonal, 2=up, 3=left
        
        max_score = 0
        max_i, max_j = 0, 0
        for i in range(1, rows):
            for j in range(1, cols):
                diag = score_matrix[i-1, j-1] + self.calculate_match_score(seq1[i-1], seq2_sets[j-1])
                up = score_matrix[i-1, j] + (self.gap_open if traceback[i-1, j] != 2 else self.gap_extend)
                left = score_matrix[i, j-1] + (self.gap_open if traceback[i, j-1] != 3 else self.gap_extend)
                score_matrix[i, j] = max(0, diag, up, left)
                if score_matrix[i, j] == 0:
                    traceback[i, j] = 0
                elif score_matrix[i, j] == diag:
                    traceback[i, j] = 1
                elif score_matrix[i, j] == up:
                    traceback[i, j] = 2
                else:
                    traceback[i, j] = 3
                if score_matrix[i, j] > max_score:
                    max_score = score_matrix[i, j]
                    max_i, max_j = i, j

        # Traceback to recover alignment
        i, j = max_i, max_j
        align1, align2 = [], []
        while i > 0 and j > 0 and traceback[i, j] != 0:
            if traceback[i, j] == 1:  # Diagonal
                align1.append(seq1[i-1])
                align2.append(list(seq2_sets[j-1]))
                i -= 1
                j -= 1
            elif traceback[i, j] == 2:  # Up
                align1.append(seq1[i-1])
                align2.append(['-'])
                i -= 1
            else:  # Left
                align1.append('-')
                align2.append(list(seq2_sets[j-1]))
                j -= 1

        align1 = align1[::-1]
        align2 = align2[::-1]
        aligned_length = len(align1)
        
        # Compute weighted matches over aligned positions (only positions with no gap in discovered)
        weighted_matches = 0.0
        for idx in range(aligned_length):
            if align1[idx] != '-' and '-' not in align2[idx]:
                weight = 1.0 / len(align2[idx])
                weighted_matches += weight

        # Identity is computed on the aligned region.
        identity = weighted_matches / aligned_length if aligned_length > 0 else 0.0
        # Coverage is the fraction of the injected motif that is aligned.
        coverage = aligned_length / len(seq1) if len(seq1) > 0 else 0.0
        
        # Instead of penalizing harshly, apply a softened coverage penalty:
        alpha = 0.5  # Exponent to soften the penalty (tune as needed)
        similarity = identity * (coverage ** alpha)
        
        seq1_aligned = ''.join(align1)
        seq2_aligned = ''.join([nuc[0] if len(nuc)==1 else f"[{''.join(nuc)}]" for nuc in align2])
        
        return max_score, seq1_aligned, seq2_aligned, similarity, aligned_length, identity, coverage

    def execute(self, data):
        """Perform motif local alignment to identify matches and integrate the discovered p_value.
        Also logs details for debugging.
        """
        result = {}
        discovered_motifs = data.get('discovered_motifs')
        if not discovered_motifs:
            print("No discovered motifs to compare against")
            result['matched_motifs'] = []
            return result

        matched_motifs = []
        debug_logs = []
        for motif in discovered_motifs:
            motif_copy = dict(motif)
            try:
                consensus = motif_copy.get('consensus', '')
                if not consensus:
                    continue
                consensus_sets = self.parse_consensus_to_sets(consensus)
                (score, aligned_injected, aligned_discovered, similarity,
                aligned_length, identity, coverage) = self.smith_waterman(self.injected_motif, consensus_sets)
                motif_copy['alignment_score'] = score
                motif_copy['similarity_score'] = similarity
                motif_copy['is_match'] = similarity >= self.similarity_threshold
                try:
                    p_val = float(motif_copy.get('p_value', 'inf'))
                except Exception:
                    p_val = None
                motif_copy['p_value'] = p_val
                motif_copy['significant'] = (p_val is not None and p_val < 1e-10)
                motif_copy['alignment'] = {
                    'injected': aligned_injected,
                    'discovered': aligned_discovered
                }
                matched_motifs.append(motif_copy)
                debug_logs.append({
                    'consensus': consensus,
                    'aligned_injected': aligned_injected,
                    'aligned_discovered': aligned_discovered,
                    'aligned_length': aligned_length,
                    'identity': round(identity, 3),
                    'coverage': round(coverage, 3),
                    'similarity': round(similarity, 3),
                    'p_value': p_val,
                    'significant': motif_copy['significant']
                })
            except Exception as e:
                print(f"Error processing motif {motif_copy.get('id', 'unknown')}: {str(e)}")
                motif_copy['is_match'] = False
                motif_copy['similarity_score'] = 0.0
                motif_copy['p_value'] = None
                motif_copy['significant'] = False
                motif_copy['error'] = str(e)
                matched_motifs.append(motif_copy)

        matched_motifs.sort(key=lambda m: ((not m.get('significant', False)), -m.get('similarity_score', 0.0)))
        result['matched_motifs'] = matched_motifs
        
        print("Local Alignment Batch Summary:")
        for log in debug_logs:
            print(f"Consensus: {log['consensus']} | Identity: {log['identity']} | Coverage: {log['coverage']} | "
                f"Aligned Length: {log['aligned_length']} | Similarity: {log['similarity']} | "
                f"p_value: {log['p_value']} | Significant: {log['significant']}")
        
        return result

import numpy as np
import math
from flowline import FlowPipe

# Ensure these globals are defined or imported from elsewhere
NUCLEOTIDES = ['A', 'C', 'G', 'T']

def reverse_complement_pwm(pwm):
    """
    Given a PWM as a dictionary with keys 'A', 'C', 'G', 'T',
    return the reverse complement PWM.
    Reverse the order of columns and swap A <-> T and C <-> G.
    """
    # Reverse each list
    rev_pwm = {
        'A': list(reversed(pwm['A'])),
        'C': list(reversed(pwm['C'])),
        'G': list(reversed(pwm['G'])),
        'T': list(reversed(pwm['T']))
    }
    # Swap the nucleotide keys: A <-> T, C <-> G
    rc_pwm = {
        'A': rev_pwm['T'],
        'T': rev_pwm['A'],
        'C': rev_pwm['G'],
        'G': rev_pwm['C']
    }
    return rc_pwm


class PWMComparisonPipe(FlowPipe):
    """
    Pipe for comparing PWMs of injected and discovered motifs.
    
    Input:
        - injected_pwm: PWM of the injected motif (as a dictionary)
        - discovered_motifs: List of motif objects with PWMs (from MemeXmlParserPipe).
          Each motif is expected to include a 'p_value' field.
        
    Output:
        - matched_motifs: List of motifs with added properties:
             'is_match', 'similarity_score', 'alignment', 'p_value', 'significant',
             and 'orientation' (indicating "normal" or "reverse_complement").
             A motif is flagged as significant if its p_value is lower than 1e-10.
    """
    def __init__(self, method='log_odds', similarity_threshold=0.7, 
                 background_freqs={'A': 0.25, 'C': 0.25, 'G': 0.25, 'T': 0.25}):
        """Initialize the PWM comparison pipe."""
        super().__init__(inputs=["discovered_motifs", "injected_pwm"], outputs=["matched_motifs"])
        self.method = method
        self.similarity_threshold = similarity_threshold
        self.background_freqs = background_freqs
    
    def log_odds_score(self, pwm1, pwm2, bg_freqs):
        """
        Calculate the log-odds score between two PWMs.
        
        Args:
            pwm1: First PWM as dictionary.
            pwm2: Second PWM as dictionary.
            bg_freqs: Background frequencies.
            
        Returns:
            Average log-odds score across all positions.
        """
        if len(pwm1['A']) != len(pwm2['A']):
            return None
        
        width = len(pwm1['A'])
        total_score = 0
        for i in range(width):
            position_score = 0
            for nuc in NUCLEOTIDES:
                p1 = pwm1[nuc][i]
                p2 = pwm2[nuc][i]
                bg = bg_freqs[nuc]
                p1 = max(p1, 1e-6)
                p2 = max(p2, 1e-6)
                if p1 > 0:
                    position_score += p1 * np.log2(p2 / bg)
            total_score += position_score
        return total_score / width
    
    def kl_divergence(self, pwm1, pwm2):
        """
        Calculate the Kullback-Leibler divergence between two PWMs.
        
        Args:
            pwm1: First PWM as dictionary.
            pwm2: Second PWM as dictionary.
            
        Returns:
            Average KL divergence across all positions (lower is better).
        """
        if len(pwm1['A']) != len(pwm2['A']):
            return float('inf')
        
        width = len(pwm1['A'])
        total_divergence = 0
        for i in range(width):
            position_divergence = 0
            for nuc in NUCLEOTIDES:
                p1 = pwm1[nuc][i]
                p2 = pwm2[nuc][i]
                p1 = max(p1, 1e-6)
                p2 = max(p2, 1e-6)
                if p1 > 0:
                    position_divergence += p1 * np.log2(p1 / p2)
            total_divergence += position_divergence
        return 1.0 / (1.0 + total_divergence / width)
    
    def euclidean_distance(self, pwm1, pwm2):
        """
        Calculate the Euclidean distance between two PWMs.
        
        Args:
            pwm1: First PWM as dictionary.
            pwm2: Second PWM as dictionary.
            
        Returns:
            Average Euclidean distance across all positions (lower is better, transformed to similarity).
        """
        if len(pwm1['A']) != len(pwm2['A']):
            return float('inf')
        
        width = len(pwm1['A'])
        total_distance = 0
        for i in range(width):
            position_distance = 0
            for nuc in NUCLEOTIDES:
                p1 = pwm1[nuc][i]
                p2 = pwm2[nuc][i]
                position_distance += (p1 - p2) ** 2
            total_distance += np.sqrt(position_distance)
        return 1.0 - (total_distance / (width * np.sqrt(2)))
    
    def find_best_alignment(self, pwm1, pwm2, method):
        """
        Find the best alignment between two PWMs by sliding the shorter one along the longer one.
        
        Args:
            pwm1: First PWM as dictionary.
            pwm2: Second PWM as dictionary.
            method: Comparison method to use.
            
        Returns:
            Tuple of (best_score, offset).
        """
        width1 = len(pwm1['A'])
        width2 = len(pwm2['A'])
        if width2 > width1:
            pwm1, pwm2 = pwm2, pwm1
            width1, width2 = width2, width1
            swapped = True
        else:
            swapped = False
        
        best_score = float('-inf') if method == 'log_odds' else 0
        best_offset = 0
        
        for offset in range(width1 - width2 + 1):
            aligned_pwm1 = {nuc: pwm1[nuc][offset:offset+width2] for nuc in NUCLEOTIDES}
            if method == 'log_odds':
                score = self.log_odds_score(aligned_pwm1, pwm2, self.background_freqs)
                if score is not None and best_score is not None and score > best_score:
                    best_score = score
                    best_offset = offset
            elif method == 'kl_divergence':
                score = self.kl_divergence(aligned_pwm1, pwm2)
                if score is not None and best_score is not None and score > best_score:
                    best_score = score
                    best_offset = offset
            else:  # euclidean
                score = self.euclidean_distance(aligned_pwm1, pwm2)
                if score is not None and best_score is not None and score > best_score:
                    best_score = score
                    best_offset = offset
        
        if swapped:
            best_offset = -best_offset
        
        return best_score, best_offset
    
    def execute(self, data):
        """Compare PWMs to identify matches, trying both normal and reverse-complement orientations.
           The best alignment is selected, and p_value and significance are propagated.
        """
        result = {}
        discovered_motifs = data.get('discovered_motifs')
        injected_pwm = data.get('injected_pwm')
        
        if not discovered_motifs:
            print("No discovered motifs to compare against")
            result['matched_motifs'] = []
            return result
        
        if not injected_pwm:
            raise ValueError("Missing required input: injected_pwm")
        
        matched_motifs = []
        for motif in discovered_motifs:
            motif_copy = dict(motif)
            if 'pwm' not in motif_copy or not all(key in motif_copy['pwm'] for key in NUCLEOTIDES):
                motif_copy['is_match'] = False
                motif_copy['similarity_score'] = 0.0
                motif_copy['error'] = "Missing or invalid PWM"
                try:
                    motif_copy['p_value'] = float(motif_copy.get('p_value', 'inf'))
                except Exception:
                    motif_copy['p_value'] = None
                motif_copy['significant'] = False
                matched_motifs.append(motif_copy)
                continue
            
            try:
                # Compute similarity for normal orientation.
                score_normal, offset_normal = self.find_best_alignment(injected_pwm, motif_copy['pwm'], self.method)
                # Compute similarity for reverse complement orientation.
                rc_pwm = reverse_complement_pwm(motif_copy['pwm'])
                score_rc, offset_rc = self.find_best_alignment(injected_pwm, rc_pwm, self.method)
                
                # Choose the orientation with the higher similarity score.
                if score_rc > score_normal:
                    best_score = score_rc
                    best_offset = offset_rc
                    orientation = "reverse_complement"
                else:
                    best_score = score_normal
                    best_offset = offset_normal
                    orientation = "normal"
                
                motif_copy['similarity_score'] = best_score
                motif_copy['alignment'] = best_offset
                motif_copy['orientation'] = orientation
                motif_copy['is_match'] = best_score >= self.similarity_threshold
                
                try:
                    p_val = float(motif_copy.get('p_value', 'inf'))
                except Exception:
                    p_val = None
                motif_copy['p_value'] = p_val
                motif_copy['significant'] = (p_val is not None and p_val < 1e-10)
                
                matched_motifs.append(motif_copy)
            except Exception as e:
                print(f"Error processing PWM for motif {motif_copy.get('id', 'unknown')}: {str(e)}")
                motif_copy['is_match'] = False
                motif_copy['similarity_score'] = 0.0
                motif_copy['p_value'] = None
                motif_copy['significant'] = False
                motif_copy['error'] = str(e)
                matched_motifs.append(motif_copy)
        
        # Sort: significant motifs first, then by descending similarity score.
        matched_motifs.sort(key=lambda m: ((not m.get('significant', False)), -m.get('similarity_score', 0.0)))
        result['matched_motifs'] = matched_motifs
        return result

class MotifSummaryPipe(FlowPipe):
    """
    Pipe for processing matched motifs and integrating them with a summary record.
    
    Input:
        - matched_motifs: List of motif objects with similarity scores and alignments
        - summary_record: A single record containing data about the motif analysis run
        
    Output:
        - enriched_record: The summary record enriched with motif match information
    """
    def __init__(self):
        """Initialize the motif summary pipe."""
        inputs = ["matched_motifs", "summary_record"]
        outputs = ["enriched_record"]
        super().__init__(inputs=inputs, outputs=outputs, action=self._process_summary)
    
    @staticmethod
    def sort_key(m):
        p_val = m.get('p_value')
        # A motif is significant if p_value exists and is < 1e-10.
        significant = (p_val is not None and p_val < 1e-10)
        # For the key:
        #   First element: 0 if significant, 1 if not (so that significant ones come first)
        #   Second element: for significant motifs, use the p_value (ascending, lower is better),
        #                   for non-significant motifs, we can default to 0.
        #   Third element: negative similarity so that higher similarity comes first.
        return (0 if significant else 1, p_val if significant and p_val is not None else 0, -m.get('similarity_score', 0.0))
    
    def _process_summary(self, data):
        """Process matched motifs and integrate with the summary record."""
        result = {}
        
        # Get required inputs
        matched_motifs = data.get('matched_motifs', [])
        summary_record = data.get('summary_record')
        
        # Validate required inputs
        if not matched_motifs:
            print("No matched motifs to process")
        if not summary_record:
            raise ValueError("Missing required input: summary_record")
        
        # Make a copy of the summary record to avoid modifying the original
        target_record = summary_record.copy()
        
        # Sort matched motifs by similarity score (descending) to find the best match first
        matched_motifs = sorted(matched_motifs, key=self.sort_key)
        # Track if we found any matches
        found_match = False
        extra_match_info = []
        
        # Process the sorted motifs
        for motif in matched_motifs:
            # Extract relevant information
            is_match = motif.get('is_match', False)
            similarity_score = motif.get('similarity_score', 0.0)
            alignment = motif.get('alignment', {})
            consensus = motif.get('consensus', "")
            p_value = motif.get('p_value', None)
            significant = motif.get('significant', False)
            
            if is_match:
                # If this is the first match (highest similarity), add it directly to the record
                if not found_match:
                    target_record['is_match'] = True
                    target_record['is_significant'] = significant
                    target_record['p_value'] = p_value
                    target_record['similarity_score'] = similarity_score
                    target_record['alignment'] = alignment
                    target_record['matched_consensus'] = consensus
                    found_match = True
                else:
                    # For additional matches, add to extra_match_info
                    extra_match_info.append({
                        'consensus': consensus,
                        'similarity_score': similarity_score,
                        'alignment': alignment,
                        'p_value': p_value,
                        'significant': significant
                    })
            elif 'error' in motif:
                # If there was an error processing this motif, add it to extra_match_info
                extra_match_info.append({
                    'error': motif['error'],
                    'consensus': consensus
                })
        
        # If no match was found, set is_match to False
        if not found_match:
            target_record['is_match'] = False
            target_record['is_significant'] = False
            target_record['p_value'] = None
            target_record['similarity_score'] = 0.0
            target_record['alignment'] = None
            target_record['matched_consensus'] = None
        
        # Add the extra match info to the record
        target_record['extra_match_info'] = extra_match_info
        
        # Return the enriched record
        result['enriched_record'] = target_record
        
        return result

# class HomerTextParserPipe(FlowPipe):
#     """
#     Pipe for parsing HOMER text results files to extract detailed motif information.
    
#     Input:
#         - output_dir: Directory containing HOMER output (with homer.txt)
#         - status: Job execution status (must be "COMPLETED" to proceed)
        
#     Output:
#         - motifs: List of motif objects with detailed information:
#             {
#                 'id': motif ID (e.g. 'motif_1'),
#                 'name': motif name,
#                 'consensus': consensus string,
#                 'width': width of the motif,
#                 'p_value': p-value of the motif,
#                 'sites': number of sites,
#                 'pwm': position weight matrix as a dict with keys for A, C, G, T
#                 'locations': list of locations where motif was found
#             }
#     """
#     def __init__(self):
#         """Initialize the HOMER text parser pipe."""
#         inputs = ["output_dir", "status"]
#         outputs = ["motifs"]
#         super().__init__(inputs=inputs, outputs=outputs, action=self._parse_homer_text)
    
#     def _parse_homer_text(self, data):
#         """Parse HOMER text results to extract detailed motif information."""
#         result = {}
        
#         # Get required inputs
#         output_dir = data.get('output_dir')
#         status = data.get('status')
        
#         # Validate required inputs
#         if not output_dir:
#             raise ValueError("Missing required input: output_dir")
#         if not status:
#             raise ValueError("Missing required input: status")
            
#         # Check if the job execution was successful
#         if status != "COMPLETED":
#             raise ValueError(f"Cannot parse HOMER results: job status is {status}, expected COMPLETED")
        
#         # Check if the output directory exists
#         if not os.path.isdir(output_dir):
#             print(f"Output directory does not exist: {output_dir}")
#             raise ValueError(f"Output directory does not exist: {output_dir}")
        
#         # Check for the homer.txt file, which contains the results
#         homer_txt_path = os.path.join(output_dir, "homer.txt")
#         if not os.path.isfile(homer_txt_path):
#             print(f"HOMER text file not found: {homer_txt_path}")
#             raise ValueError(f"HOMER text file not found: {homer_txt_path}")
        
#         # Parse the text file
#         try:
#             with open(homer_txt_path, 'r') as f:
#                 lines = f.readlines()
            
#             # List to store parsed motifs
#             motifs = []
#             current_motif = None
#             pwm_lines = []
            
#             for line in lines:
#                 line = line.strip()
#                 if not line:
#                     continue
                
#                 # Check if this is a motif header line (starts with '>')
#                 if line.startswith('>'):
#                     # If we were processing a motif, add it to the list
#                     if current_motif:
#                         # Process PWM lines
#                         pwm = self._parse_pwm_lines(pwm_lines)
#                         current_motif['pwm'] = pwm
#                         motifs.append(current_motif)
                    
#                     # Start a new motif
#                     parts = line[1:].split('\t')
#                     motif_id = parts[0]
#                     motif_name = parts[1]
                    
#                     # Extract statistics from the header
#                     stats = {}
#                     for stat in parts[2:]:
#                         if ':' in stat:
#                             key, value = stat.split(':', 1)
#                             stats[key] = value
                    
#                     # Create the motif object
#                     current_motif = {
#                         'id': motif_id,
#                         'name': motif_name,
#                         'consensus': motif_id,  # Use the motif ID as consensus
#                         'width': len(motif_id),  # Width is the length of the motif ID
#                         'p_value': stats.get('P', '1'),  # Default to 1e-10 if not provided
#                         'sites': int(stats.get('T', '-1').split('.')[0]),  # Extract number from T:175.0(35.00%)
#                     }
                    
#                     # Reset PWM lines
#                     pwm_lines = []
#                 else:
#                     # This is a PWM line
#                     pwm_lines.append(line)
            
#             # Add the last motif if there is one
#             if current_motif:
#                 pwm = self._parse_pwm_lines(pwm_lines)
#                 current_motif['pwm'] = pwm
#                 motifs.append(current_motif)
            
#             # Add motifs to result
#             result['motifs'] = motifs
            
#         except Exception as e:
#             print(f"Error parsing HOMER text: {str(e)}")
#             raise ValueError(f"Error parsing HOMER text: {str(e)}")
        
#         # Return results
#         return result
    
#     def _parse_pwm_lines(self, pwm_lines):
#         """Parse PWM lines into a dictionary format."""
#         pwm = {'A': [], 'C': [], 'G': [], 'T': []}
        
#         for line in pwm_lines:
#             values = line.split('\t')
#             if len(values) != 4:
#                 continue
            
#             # Add probabilities for each nucleotide
#             pwm['A'].append(float(values[0]))
#             pwm['C'].append(float(values[1]))
#             pwm['G'].append(float(values[2]))
#             pwm['T'].append(float(values[3]))
        
#         return pwm

# parserData = MemeXmlParserPipe().execute({'output_dir': 'meme_output_test_dir_40_50pct_run1', 'status': 'COMPLETED'})
# localAlignmentData = MotifLocalAlignmentPipe(injected_motif='AAACCCTTTGGG').execute({
#     'discovered_motifs': parserData['motifs']})
# matched_motifs = [ motif for motif in localAlignmentData['matched_motifs'] if motif['is_match'] ]
# oneshotPwm = StringToOneShotPWMPipe().execute({'motif_string': 'AAACCCTTTGGG'})
# pwmComparisonData = PWMComparisonPipe(injected_pwm=oneshotPwm['pwm']).execute({
#     'discovered_motifs': parserData['motifs']})
# matched_motifs = [ motif for motif in pwmComparisonData['matched_motifs'] if motif['is_match'] ]
# print("Hello")
